use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// ConnectForge — a durable, partitioned, segmented commit-log broker.
#[derive(Debug, Default, Parser)]
#[command(name = "connectforge-node", version, about)]
pub struct Cli {
    /// Subcommand to run (defaults to `serve`).
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Top-level commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the GraphQL + WebSocket server.
    Serve(ServeArgs),
    /// Run an in-process produce/fetch demo and print a summary.
    Demo(DemoArgs),
}

/// Arguments for the `serve` command.
#[derive(Debug, Parser)]
pub struct ServeArgs {
    /// Address to bind the HTTP server to.
    #[arg(long, env = "CONNECTFORGE_ADDR", default_value = "0.0.0.0:8080")]
    pub addr: SocketAddr,

    /// Directory for the durable segmented store. If unset, an in-memory store
    /// is used (data is not persisted across restarts).
    #[arg(long, env = "CONNECTFORGE_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Maximum number of topics.
    #[arg(long, env = "CONNECTFORGE_MAX_TOPICS", default_value_t = 1024)]
    pub max_topics: usize,

    /// Per-subscriber broadcast buffer depth.
    #[arg(long, env = "CONNECTFORGE_SUB_BUFFER", default_value_t = 4_096)]
    pub subscriber_buffer: usize,
}

/// Arguments for the `demo` command.
#[derive(Debug, Parser)]
pub struct DemoArgs {
    /// Topic name to create and use.
    #[arg(long, default_value = "demo")]
    pub topic: String,

    /// Number of partitions.
    #[arg(long, default_value_t = 4)]
    pub partitions: u32,

    /// Total records to produce.
    #[arg(long, default_value_t = 100_000)]
    pub records: u64,

    /// Number of distinct keys.
    #[arg(long, default_value_t = 1_000)]
    pub keys: u64,

    /// Dead-letter every Nth offset to exercise the DLQ path (0 = never fail).
    #[arg(long, default_value_t = 1_000)]
    pub fail_every: u64,

    /// Optional directory to exercise the durable store instead of memory.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_defaults_parse() {
        let cli = Cli::parse_from(["connectforge-node", "serve"]);
        match cli.command.unwrap() {
            Command::Serve(a) => {
                assert_eq!(a.max_topics, 1024);
                assert!(a.data_dir.is_none());
            }
            Command::Demo(_) => panic!("expected serve"),
        }
    }

    #[test]
    fn demo_args_parse() {
        let cli = Cli::parse_from([
            "connectforge-node",
            "demo",
            "--records",
            "5",
            "--partitions",
            "2",
        ]);
        match cli.command.unwrap() {
            Command::Demo(a) => {
                assert_eq!(a.records, 5);
                assert_eq!(a.partitions, 2);
            }
            Command::Serve(_) => panic!("expected demo"),
        }
    }
}
