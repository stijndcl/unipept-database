use std::fs::File;
use std::io::{BufRead, BufReader, Lines};
use std::path::PathBuf;
use clap::Parser;
use unipept::utils::files::{open_read, open_sin};

fn main() {
    let args = Cli::parse();

    let sequences = open_sin();
    let mut lcas_original = LineRetriever::new(&args.lcas_original);
    let mut lcas_eq = LineRetriever::new(&args.lcas_equalized);
    let mut fas_original = LineRetriever::new(&args.fas_original);
    let mut fas_eq = LineRetriever::new(&args.fas_equalized);

    for line in sequences.lines() {
        match line {
            Err(e) => {
                eprintln!("error reading sequence line: {:?}", e);
                std::process::exit(1);
            },
            Ok(l) => {
                let (id, sequence_data) = split_line(l);
                let olca = lcas_original.get(id);
                let elca = lcas_eq.get(id);
                let ofas = fas_original.get(id);
                let efas = fas_eq.get(id);

                println!("{}\t{}\t{}\t{}\t{}\t{}", id, sequence_data, olca, elca, ofas, efas);
            },
        }
    }
}

/// The amount of digits that the input TSV files are padded with
const ID_PADDING: usize = 12;

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

/// Split a line into the first 12 characters (parsed as an id), and the rest
fn split_line(line: String) -> (u64, String) {
    let (id, mut data) = line.split_at(ID_PADDING);
    data = data.trim();

    match id.parse::<u64>() {
        Ok(v) => (v, data.to_string()),
        Err(e) => {
            eprintln!("unable to parse {} to u64: {:?}", id, e);
            std::process::exit(1);
        }
    }
}

/// Struct that returns a line if it starts with the required id,
/// otherwise None, and holds on to the id
struct LineRetriever {
    lines: Lines<BufReader<File>>,
    buffered_id: u64,
    buffered_line: Option<String>,
}

impl LineRetriever {
    pub fn new(pb: &PathBuf) -> Self {
        LineRetriever {
            lines: open_read(pb).lines(),
            buffered_id: 0,
            buffered_line: None,
        }
    }

    /// Get the next line from the buffer if the id matches, otherwise \\N
    pub fn get(&mut self, required_id: u64) -> String {
        // Check if there's a line in the buffer first
        match self.buffered_line.clone() {
            Some(l) => {
                // The line we have is the one required -> clear buffer and return the value
                if self.buffered_id == required_id {
                    self.buffered_id = 0;
                    self.buffered_line = None;
                    return l;
                }

                // We don't have the required line yet, so return NULL character
                "\\N".to_string()
            }
            None => {
                let line = self.lines.next();

                let line_content = match line {
                    None => return "\\N".to_string(),
                    Some(l) => l.expect("unable to read line")
                };

                let (id, data) = split_line(line_content);

                // The line we just read is the line we need
                if id == required_id {
                    return data.to_string();
                }

                // Otherwise, store it for later and return NULL character
                self.buffered_id = id;
                self.buffered_line = Some(data.to_string());
                "\\N".to_string()
            }
        }
    }
}

