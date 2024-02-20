use anyhow::{Context, Result};

use clap::Parser;
use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;
use unipept_database::utils::files::open_read;

fn main() -> Result<()> {
    let args = Cli::parse();
    let peptides_infile = open_read(&args.peptides).context("Error opening peptides input file")?;
    let sequences_infile =
        open_read(&args.sequences).context("Error opening sequences input file")?;

    let mut peptides_reader = peptides_infile.lines();
    let mut sequences_reader = sequences_infile.lines();

    let mut sequence_to_id: HashMap<String, u64> = HashMap::new();

    let mut current_id: u64 = u64::MAX;
    let mut current_sequence = String::new();
    let mut current_peptide = String::new();
    let mut current_eq = String::new();
    let mut current_og = String::new();

    for prefix in 'A'..='Z' {
        // Group everything between I and L together
        if prefix > 'I' && prefix <= 'L' {
            continue;
        }

        if !current_peptide.is_empty() && !eq_or_og_starts_with(&current_eq, &current_og, prefix) {
            continue;
        }

        // Add the last line that we read too much in the previous iteration if there is one
        if !current_sequence.is_empty() && sequence_starts_with(&current_sequence, prefix) {
            sequence_to_id.insert(current_sequence, current_id);
            current_id = u64::MAX;
            current_sequence = String::new();
        }

        // First find all sequences that start with this letter
        loop {
            let line = sequences_reader.next();
            let line = match line {
                None => break,
                Some(l) => l.context("Error reading line from sequences file")?,
            };

            let (id, sequence) = line.split_once('\t').context("Error splitting line")?;
            let id_parsed = id
                .parse()
                .with_context(|| format!("Error parsing {id} as an integer"))?;

            if sequence_starts_with(sequence, prefix) {
                sequence_to_id.insert(sequence.to_string(), id_parsed);
            } else {
                current_id = id_parsed;
                current_sequence = sequence.to_string();
                break;
            }
        }

        if !current_peptide.is_empty() {
            let data: Vec<&str> = current_peptide.splitn(4, '\t').collect();
            let eq_id = sequence_to_id
                .get(data[1])
                .with_context(|| format!("Missing sequence {}", data[1]))?;
            let orig_id = sequence_to_id
                .get(data[2])
                .with_context(|| format!("Missing sequence {}", data[2]))?;
            println!("{}\t{}\t{}\t{}", data[0], eq_id, orig_id, data[3]);

            current_peptide = String::new();
            current_eq = String::new();
            current_og = String::new();
        }

        // Then find all peptide lines that contain these sequences
        loop {
            let line = peptides_reader.next();
            let line = match line {
                None => break,
                Some(l) => l.context("Error reading line from peptides file")?,
            };

            let data: Vec<&str> = line.splitn(4, '\t').collect();

            if eq_or_og_starts_with(data[1], data[2], prefix) {
                let eq_id = sequence_to_id
                    .get(data[1])
                    .with_context(|| format!("Missing sequence {}", data[1]))?;
                let orig_id = sequence_to_id
                    .get(data[2])
                    .with_context(|| format!("Missing sequence {}", data[2]))?;

                println!("{}\t{}\t{}\t{}", data[0], eq_id, orig_id, data[3]);
            } else {
                current_eq = data[1].to_string();
                current_og = data[2].to_string();
                current_peptide = line;
                break;
            }
        }

        sequence_to_id.clear();
    }

    Ok(())
}

#[derive(Parser, Debug)]
struct Cli {
    #[clap(long)]
    peptides: PathBuf,
    #[clap(long)]
    sequences: PathBuf,
}

fn eq_or_og_starts_with(eq: &str, og: &str, character: char) -> bool {
    sequence_starts_with(eq, character) || sequence_starts_with(og, character)
}

fn sequence_starts_with(seq: &str, character: char) -> bool {
    let first = seq.chars().next().unwrap();

    // Special case: treat I-L as one
    if character == 'I' {
        return ('I'..='L').contains(&first);
    }

    first == character
}
