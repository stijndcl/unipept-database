use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

const FILENAME: &str = "datasets-meta.json";

/// CLI-tool to help tracking the state of downloaded and processed datasets
fn main() -> Result<()> {
    let cli = Cli::parse();
    let filepath = cli.index_dir.join(FILENAME);
    let mut meta = load_metadata(&filepath)?;

    match cli.cmd {
        Commands::Delete(args) => {
            cmd_delete(&mut meta, args);
            meta.write(filepath).context("Error deleting E-Tag")?;
        }
        Commands::Get(args) => {
            let etag = cmd_get(&meta, args);
            match etag {
                None => { println!() }
                Some(tag) => { println!("{tag}") }
            }
        }
        Commands::Set(args) => {
            cmd_set(&mut meta, args);
            meta.write(filepath).context("Error storing E-Tag")?;
        }
        Commands::ShouldReprocess(args) => {
            let reprocess = cmd_should_reprocess(meta, args);
            println!("{}", reprocess);
        }
    }

    Ok(())
}

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,

    #[clap(short, long, env = "INDEX_DIR")]
    index_dir: PathBuf,
}

#[derive(Args, Clone, Debug)]
struct DeleteArgs {
    stage: ProcessingStage,
    db_type: String,
}

#[derive(Args, Clone, Debug)]
struct GetArgs {
    stage: ProcessingStage,
    db_type: String,
}

#[derive(Args, Clone, Debug)]
struct SetArgs {
    stage: ProcessingStage,
    db_type: String,
    value: String,
}

#[derive(Args, Clone, Debug)]
struct ShouldReprocessArgs {
    db_type: String,
    db_source: String
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Delete(DeleteArgs),
    Get(GetArgs),
    Set(SetArgs),
    ShouldReprocess(ShouldReprocessArgs),
}

#[derive(ValueEnum, Debug, Clone)]
enum ProcessingStage {
    Downloaded,
    Processed,
}

#[derive(Serialize, Deserialize)]
struct DatasetsMetadata {
    downloaded: HashMap<String, String>,
    processed: HashMap<String, String>,
}

impl DatasetsMetadata {
    fn new() -> Self {
        return DatasetsMetadata {
            downloaded: HashMap::new(),
            processed: HashMap::new(),
        };
    }

    /// Write the contents of the metadata struct to a file.
    fn write(&self, filepath: PathBuf) -> Result<()> {
        let mut file = OpenOptions::new().write(true).open(filepath).context("Error opening metadata file")?;
        let contents = serde_json::to_string(self).context("Error serializing DatasetsMetadata struct")?;
        file.write_all(contents.as_bytes()).context("Error writing to metadata file")
    }
}

/// Load and deserialize the metadata file.
/// If the file does not exist, it is first created and filled with an empty instance.
fn load_metadata(filepath: &PathBuf) -> Result<DatasetsMetadata> {
    if filepath.exists() {
        let contents = fs::read_to_string(filepath).context("Error reading metadata file")?;
        let deserialized = serde_json::from_str(contents.as_str()).context("Error deserializing file contents")?;

        return Ok(deserialized);
    }

    // File doesn't exist yet, create it first and then return an empty struct
    let mut file = File::create(filepath).context("Error creating metadata file")?;
    let empty = DatasetsMetadata::new();
    let contents = serde_json::to_string(&empty).context("Error serializing empty DatasetsMetadata struct")?;
    file.write_all(contents.as_bytes()).context("Error writing to metadata file")?;
    Ok(empty)
}

fn cmd_delete(meta: &mut DatasetsMetadata, args: DeleteArgs) {
    match args.stage {
        ProcessingStage::Downloaded => { meta.downloaded.remove(&args.db_type); }
        ProcessingStage::Processed => { meta.processed.remove(&args.db_type); }
    }
}

fn cmd_get(meta: &DatasetsMetadata, args: GetArgs) -> Option<&String> {
    match args.stage {
        ProcessingStage::Downloaded => meta.downloaded.get(&args.db_type),
        ProcessingStage::Processed => meta.processed.get(&args.db_type)
    }
}

fn cmd_set(meta: &mut DatasetsMetadata, args: SetArgs) {
    match args.stage {
        ProcessingStage::Downloaded => {
            meta.downloaded.insert(args.db_type, args.value);
        }
        ProcessingStage::Processed => {
            meta.processed.insert(args.db_type, args.value);
        }
    }
}

fn cmd_should_reprocess(meta: DatasetsMetadata, args: ShouldReprocessArgs) -> bool {
    // The REST-API does not provide E-Tags, so these should always be re-processed
    if args.db_source.contains("rest") {
        return true;
    }

    // If we have not downloaded this yet, process
    // In practice this is not possible, it should have been downloaded a few steps before
    let downloaded = meta.downloaded.get(&args.db_type);
    let downloaded = match downloaded {
        None => { return true; }
        Some(d) => { d }
    };

    // If we have not processed this yet, process
    let processed = meta.processed.get(&args.db_type);
    let processed = match processed {
        None => { return true; }
        Some(p) => { p }
    };

    // Re-process if the downloaded and processed one don't match
    return downloaded != processed;
}
