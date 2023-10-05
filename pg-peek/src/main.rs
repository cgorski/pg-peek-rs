use clap::{Parser, Subcommand};
use pg_peek_lib::{get_system_endianness, read_all_pages};
use std::fs::File;

#[derive(Parser, Debug)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

/// Subcommands available
#[derive(Subcommand, Debug)]
enum Commands {
    /// Subcommand for handling tables
    Table {
        #[arg(short, long)]
        filename: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Table { filename } => {
            let mut file = File::open(filename)?;
            let endianness = get_system_endianness();
            let header = read_all_pages(&mut file, endianness)?;
            println!("{:#?}", header);
        }
    }

    Ok(())
}
