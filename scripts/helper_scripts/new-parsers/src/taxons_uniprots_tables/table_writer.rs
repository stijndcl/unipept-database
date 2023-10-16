use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use crate::taxons_uniprots_tables::models::Entry;
use crate::taxons_uniprots_tables::taxon_list::TaxonList;
use crate::taxons_uniprots_tables::utils::{now, open_write};

/// Note: this is single-threaded
///       we attempted a parallel version that wrote to all files at the same time,
///       but this didn't achieve any speed increase, so we decided not to go forward with it
pub struct TableWriter {
    taxons: TaxonList,
    wrong_ids: HashSet<i32>,
    peptides: BufWriter<File>,
    uniprot_entries: BufWriter<File>,
    go_cross_references: BufWriter<File>,
    ec_cross_references: BufWriter<File>,
    ip_cross_references: BufWriter<File>,

    peptide_count: i64,
    uniprot_count: i64,
    go_count: i64,
    ec_count: i64,
    ip_count: i64,
}

impl TableWriter {
    pub fn new(taxons: &PathBuf, peptides: &PathBuf, uniprot_entries: &PathBuf, go_references: &PathBuf, ec_references: &PathBuf, interpro_references: &PathBuf) -> Self {
        TableWriter {
            taxons: TaxonList::from_file(taxons),
            wrong_ids: HashSet::new(),
            peptides: open_write(peptides),
            uniprot_entries: open_write(uniprot_entries),
            go_cross_references: open_write(go_references),
            ec_cross_references: open_write(ec_references),
            ip_cross_references: open_write(interpro_references),

            peptide_count: 0,
            uniprot_count: 0,
            go_count: 0,
            ec_count: 0,
            ip_count: 0,
        }
    }

    // Store a complete entry in the database
    pub fn store(&mut self, mut entry: Entry) {
        let id = self.add_uniprot_entry(&entry);

        // Failed to add entry
        if id == -1 { return; }

        for r in &entry.go_references {
            self.add_go_ref(r.clone(), id);
        }

        for r in &entry.ec_references {
            self.add_ec_ref(r.clone(), id);
        }

        for r in &entry.ip_references {
            self.add_ip_ref(r.clone(), id);
        }

        let digest = entry.digest();
        let go_ids = entry.go_references.into_iter();
        let ec_ids = entry.ec_references.iter().filter(|x| !x.is_empty()).map(|x| format!("EC:{}", x)).into_iter();
        let ip_ids = entry.ip_references.iter().filter(|x| !x.is_empty()).map(|x| format!("IPR:{}", x)).into_iter();

        let summary = go_ids.chain(ec_ids).chain(ip_ids).collect::<Vec<String>>().join(";");

        for sequence in digest {
            self.add_peptide(sequence.replace("I", "L"), id, sequence, summary.clone());
        }
    }

    fn add_peptide(&mut self, sequence: String, id: i64, original_sequence: String, annotations: String) {
        self.peptide_count += 1;

        if let Err(e) = writeln!(
            &mut self.peptides,
            "{}\t{}\t{}\t{}\t{}",
            self.peptide_count,
            sequence, original_sequence,
            id, annotations
        ) {
            eprintln!("{}\tError writing to CSV.\n{:?}", now(), e);
        }
    }

    // Store the entry info and return the generated id
    fn add_uniprot_entry(&mut self, entry: &Entry) -> i64 {
        if 0 <= entry.taxon_id && entry.taxon_id < self.taxons.len() as i32 && self.taxons.get(entry.taxon_id as usize).is_some() {
            self.uniprot_count += 1;

            let accession_number = entry.accession_number.clone();
            let version = entry.version.clone();
            let taxon_id = entry.taxon_id;
            let type_ = entry.type_.clone();
            let name = entry.name.clone();
            let sequence = entry.sequence.clone();

            if let Err(e) = writeln!(
                &mut self.uniprot_entries,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                self.uniprot_count, accession_number,
                version, taxon_id,
                type_, name,
                sequence
            ) {
                eprintln!("{}\tError writing to CSV.\n{:?}", now(), e);
            } else {
                return self.uniprot_count;
            }
        } else {
            if !self.wrong_ids.contains(&entry.taxon_id) {
                self.wrong_ids.insert(entry.taxon_id);
                eprintln!(
                    "{}\t{} added to the list of {} invalid taxonIds.",
                    now(),
                    entry.taxon_id,
                    self.wrong_ids.len()
                );
            }
        }

        -1
    }

    fn add_go_ref(&mut self, ref_id: String, uniprot_entry_id: i64) {
        self.go_count += 1;

        if let Err(e) = writeln!(
            &mut self.go_cross_references,
            "{}\t{}\t{}",
            self.go_count, uniprot_entry_id, ref_id
        ) {
            eprintln!("{}\tError adding GO reference to the database.\n{:?}", Instant::now().elapsed().as_millis(), e);
        }
    }

    fn add_ec_ref(&mut self, ref_id: String, uniprot_entry_id: i64) {
        self.ec_count += 1;

        if let Err(e) = writeln!(
            &mut self.ec_cross_references,
            "{}\t{}\t{}",
            self.ec_count, uniprot_entry_id, ref_id
        ) {
            eprintln!("{}\tError adding EC reference to the database.\n{:?}", Instant::now().elapsed().as_millis(), e);
        }
    }

    fn add_ip_ref(&mut self, ref_id: String, uniprot_entry_id: i64) {
        self.ip_count += 1;

        if let Err(e) = writeln!(
            &mut self.ip_cross_references,
            "{}\t{}\t{}",
            self.ip_count, uniprot_entry_id, ref_id,
        ) {
            eprintln!("{}\tError adding InterPro reference to the database.\n{:?}", Instant::now().elapsed().as_millis(), e);
        }
    }
}