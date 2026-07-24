//! On-disk log segments: the durable unit of the storage engine.
//!
//! A partition's log is a sequence of [`Segment`]s, each a pair of files:
//! a `.log` holding length-prefixed records and (implicitly) a dense in-memory
//! offset index rebuilt by scanning the `.log` on open. Segments are rolled
//! once they reach the configured record count and pruned wholesale by
//! retention. All I/O here is synchronous and is expected to run inside
//! `spawn_blocking` at the store layer so the async executor is never blocked.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::{TimeZone, Utc};
use connectforge_types::{Offset, Record, SegmentId};

/// The filename suffix for a segment's record log.
const LOG_SUFFIX: &str = "log";

/// Encode a single record into `buf` using a self-describing, length-prefixed
/// little-endian framing.
fn encode(record: &Record, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&record.offset.value().to_le_bytes());
    buf.extend_from_slice(&record.timestamp.timestamp_millis().to_le_bytes());
    match &record.key {
        Some(k) => {
            buf.push(1);
            let kb = k.as_bytes();
            buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            buf.extend_from_slice(kb);
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&(record.payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&record.payload);
}

/// Decode one record from `data` starting at `pos`, returning the record and
/// the position immediately after it. Returns `None` on a clean EOF (a
/// truncated tail is treated as EOF for crash tolerance).
fn decode(data: &[u8], pos: usize) -> Option<(Record, usize)> {
    let mut p = pos;
    let offset = u64::from_le_bytes(data.get(p..p + 8)?.try_into().ok()?);
    p += 8;
    let ts_millis = i64::from_le_bytes(data.get(p..p + 8)?.try_into().ok()?);
    p += 8;
    let has_key = *data.get(p)?;
    p += 1;
    let key = if has_key == 1 {
        let klen = u32::from_le_bytes(data.get(p..p + 4)?.try_into().ok()?) as usize;
        p += 4;
        let kb = data.get(p..p + klen)?;
        p += klen;
        Some(String::from_utf8_lossy(kb).into_owned())
    } else {
        None
    };
    let plen = u32::from_le_bytes(data.get(p..p + 4)?.try_into().ok()?) as usize;
    p += 4;
    let payload = data.get(p..p + plen)?.to_vec();
    p += plen;
    let timestamp = Utc.timestamp_millis_opt(ts_millis).single()?;
    Some((
        Record {
            offset: Offset(offset),
            timestamp,
            key,
            payload,
        },
        p,
    ))
}

/// One append-only log segment backed by a file.
pub struct Segment {
    base: u64,
    path: PathBuf,
    writer: BufWriter<File>,
    /// Dense in-memory index: byte position of each record, indexed by
    /// `offset - base`.
    index: Vec<u64>,
    write_pos: u64,
}

impl Segment {
    /// Path of the `.log` file for `base` under `dir`.
    fn log_path(dir: &Path, base: u64) -> PathBuf {
        dir.join(format!("{}.{}", SegmentId(base), LOG_SUFFIX))
    }

    /// Create a fresh, empty segment with the given base offset.
    ///
    /// # Errors
    /// Returns an I/O error if the file cannot be created.
    pub fn create(dir: &Path, base: u64) -> io::Result<Self> {
        let path = Self::log_path(dir, base);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            base,
            path,
            writer: BufWriter::new(file),
            index: Vec::new(),
            write_pos: 0,
        })
    }

    /// Open an existing segment, rebuilding its index by scanning the `.log`.
    ///
    /// A partial trailing record (from a crash mid-append) is ignored, making
    /// recovery crash-tolerant.
    ///
    /// # Errors
    /// Returns an I/O error if the file cannot be read.
    pub fn open(dir: &Path, base: u64) -> io::Result<Self> {
        let path = Self::log_path(dir, base);
        let mut data = Vec::new();
        {
            let mut f = File::open(&path)?;
            f.read_to_end(&mut data)?;
        }
        let mut index = Vec::new();
        let mut pos = 0usize;
        while let Some((_, next)) = decode(&data, pos) {
            index.push(pos as u64);
            pos = next;
        }
        let write_pos = pos as u64;
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        let mut writer = BufWriter::new(file);
        // If the tail was partial, truncate it so future appends stay contiguous.
        if write_pos < data.len() as u64 {
            writer.get_ref().set_len(write_pos)?;
            writer.seek(SeekFrom::Start(write_pos))?;
        }
        Ok(Self {
            base,
            path,
            writer,
            index,
            write_pos,
        })
    }

    /// The segment's base offset.
    #[must_use]
    pub const fn base(&self) -> u64 {
        self.base
    }

    /// Number of records stored in this segment.
    #[must_use]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether the segment holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Append a record and flush it to the OS.
    ///
    /// # Errors
    /// Returns an I/O error if the write fails.
    pub fn append(&mut self, record: &Record) -> io::Result<()> {
        let mut buf = Vec::with_capacity(32 + record.payload.len());
        encode(record, &mut buf);
        self.writer.write_all(&buf)?;
        self.writer.flush()?;
        self.index.push(self.write_pos);
        self.write_pos += buf.len() as u64;
        Ok(())
    }

    /// Read records with offset `>= from`, appending up to `remaining` of them
    /// to `out`. Returns the number of records appended.
    ///
    /// # Errors
    /// Returns an I/O error if the file cannot be read.
    pub fn read_from(
        &self,
        from: u64,
        remaining: usize,
        out: &mut Vec<Record>,
    ) -> io::Result<usize> {
        if remaining == 0 || self.index.is_empty() {
            return Ok(0);
        }
        let last = self.base + self.index.len() as u64;
        if from >= last {
            return Ok(0);
        }
        let start_idx = from.saturating_sub(self.base) as usize;
        let start_pos = self.index.get(start_idx).copied().unwrap_or(0);
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(start_pos))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let mut pos = 0usize;
        let mut added = 0usize;
        while added < remaining {
            match decode(&data, pos) {
                Some((rec, next)) => {
                    out.push(rec);
                    pos = next;
                    added += 1;
                }
                None => break,
            }
        }
        Ok(added)
    }

    /// Delete the segment's backing file (used by retention).
    ///
    /// # Errors
    /// Returns an I/O error if the file cannot be removed.
    pub fn remove(self) -> io::Result<()> {
        std::fs::remove_file(&self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn rec(offset: u64, payload: &[u8]) -> Record {
        Record {
            offset: Offset(offset),
            timestamp: Utc::now(),
            key: Some(format!("k{offset}")),
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn append_and_read_roundtrip() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        for i in 0..5 {
            seg.append(&rec(i, format!("p{i}").as_bytes())).unwrap();
        }
        assert_eq!(seg.len(), 5);
        let mut out = Vec::new();
        let n = seg.read_from(2, 10, &mut out).unwrap();
        assert_eq!(n, 3);
        assert_eq!(out[0].offset, Offset(2));
        assert_eq!(out[0].payload, b"p2");
        assert_eq!(out[0].key.as_deref(), Some("k2"));
    }

    #[test]
    fn reopen_rebuilds_index() {
        let dir = tempdir().unwrap();
        {
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            seg.append(&rec(0, b"a")).unwrap();
            seg.append(&rec(1, b"b")).unwrap();
        }
        let seg = Segment::open(dir.path(), 0).unwrap();
        assert_eq!(seg.len(), 2);
        let mut out = Vec::new();
        seg.read_from(0, 10, &mut out).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].payload, b"b");
    }

    #[test]
    fn keyless_record_roundtrips() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        let r = Record {
            offset: Offset(0),
            timestamp: Utc::now(),
            key: None,
            payload: b"nokey".to_vec(),
        };
        seg.append(&r).unwrap();
        let mut out = Vec::new();
        seg.read_from(0, 1, &mut out).unwrap();
        assert_eq!(out[0].key, None);
        assert_eq!(out[0].payload, b"nokey");
    }
}
