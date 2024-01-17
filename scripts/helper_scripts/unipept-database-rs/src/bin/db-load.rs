use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;

use unipept_database::database::DatabaseContext;
use unipept_database::database::load::parallel_load;
use unipept_database::database::schema::create_schema;

fn main() -> Result<()> {
    let args = Cli::parse();

    let ctx = Arc::new(DatabaseContext {
        user: args.db_user,
        pass: args.db_pass,
        container: args.container,
    });

    create_schema(Arc::clone(&ctx), &args.schema).context("Error applying database schema")?;
    parallel_load(Arc::clone(&ctx), &args.data_dir)
}

#[derive(Parser, Debug)]
struct Cli {
    #[clap(long)]
    data_dir: PathBuf,
    #[clap(long)]
    schema: PathBuf,
    #[clap(short = 'u', long = "user")]
    db_user: String,
    #[clap(short = 'p', long = "pass", default_value = "")]
    db_pass: String,
    #[clap(short, long, default_value = None)]
    container: Option<String>
}
