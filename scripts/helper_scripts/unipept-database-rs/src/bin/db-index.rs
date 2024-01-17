use std::sync::Arc;
use anyhow::Result;
use clap::Parser;
use unipept_database::database::DatabaseContext;
use unipept_database::database::index::parallel_index;

fn main() -> Result<()> {
    let args = Cli::parse();

    let ctx = DatabaseContext {
        user: args.db_user,
        pass: args.db_pass,
        container: args.container,
    };

    parallel_index(Arc::new(ctx))
}

#[derive(Parser, Debug)]
struct Cli {
    #[clap(short = 'u', long = "user")]
    db_user: String,
    #[clap(short = 'p', long = "pass", default_value = "")]
    db_pass: String,
    #[clap(short, long, default_value = None)]
    container: Option<String>
}