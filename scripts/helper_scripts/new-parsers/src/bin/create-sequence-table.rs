use std::path::PathBuf;
use clap::Parser;
use unipept::utils::files::{open_read, open_sin};

#[derive(Parser)]
struct Cli {
    #[clap(long)]
    lcas_equalized: PathBuf,
    #[clap(long)]
    lcas_original: PathBuf,
    #[clap(long)]
    fas_equalized: PathBuf,
    #[clap(long)]
    fas_original: PathBuf,
}

fn main() {
    let args = Cli::parse();

    let sequences = open_sin();
    let lcas_original = open_read(&args.lcas_original);
    let lcas_eq = open_read(&args.lcas_equalized);
    let fas_original = open_read(&args.fas_original);
    let fas_eq = open_read(&args.fas_equalized);


}
