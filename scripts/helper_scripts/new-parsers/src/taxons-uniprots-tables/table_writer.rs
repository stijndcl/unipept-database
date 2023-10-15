use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;
use crossbeam::channel::{bounded, Sender};

use crate::Cli;
use crate::models::Entry;
use crate::taxon_list::TaxonList;
use crate::utils::{now, open_write};

const CHANNEL_SIZE: usize = 100;

type EntryData = (i64, String);
type Writer = BufWriter<File>;

pub struct TableWriter {
    taxons: TaxonList,
    wrong_ids: HashSet<i32>,
    peptides: Option<Sender<(i64, Entry)>>,
    uniprot_entries: Writer,
    go_cross_references: Option<Sender<EntryData>>,
    ec_cross_references: Option<Sender<EntryData>>,
    ip_cross_references: Option<Sender<EntryData>>,

    peptide_count: i64,
    uniprot_count: i64,

    pub threads: Vec<JoinHandle<()>>,
}

impl TableWriter {
    /// Create a new instance of a TableWriter
    /// This spawns 4 worker threads for processing purposes
    pub fn new(cli: &Cli) -> Self {
        // Spawn all worker threads for each file type

        // Go references
        let (go_ref_sender, go_ref_receiver) = bounded::<EntryData>(CHANNEL_SIZE);

        let go = &cli.go;
        let go_clone = go.clone();

        let go_handle = thread::spawn(move || {
            let mut file = open_write(&go_clone);
            let mut count: u64 = 0;

            for (id, ref_id) in go_ref_receiver {
                count += 1;
                add_go_ref(&mut file, count, ref_id, id);
            }
        });

        // EC References
        let (ec_ref_sender, ec_ref_receiver) = bounded::<EntryData>(CHANNEL_SIZE);

        let ec = &cli.ec;
        let ec_clone = ec.clone();

        let ec_handle = thread::spawn(move || {
            let mut file = open_write(&ec_clone);
            let mut count: u64 = 0;

            for (id, ref_id) in ec_ref_receiver {
                count += 1;
                add_ec_ref(&mut file, count, ref_id, id);
            }
        });

        // InterPro references
        let (ip_ref_sender, ip_ref_receiver) = bounded::<EntryData>(CHANNEL_SIZE);

        let ip = &cli.interpro;
        let ip_clone = ip.clone();

        let ip_handle = thread::spawn(move || {
            let mut file = open_write(&ip_clone);
            let mut count: u64 = 0;

            for (id, ref_id) in ip_ref_receiver {
                count += 1;
                add_ip_ref(&mut file, count, ref_id, id);
            }
        });

        // Peptides
        let (peptide_sender, peptide_receiver) = bounded(CHANNEL_SIZE);

        let peptides = &cli.peptides;
        let peptides_clone = peptides.clone();

        let peptide_handle = thread::spawn(move || {
            let mut file = open_write(&peptides_clone);
            let mut count: u64 = 0;

            for (id, entry) in peptide_receiver {
                count += 1;
                process_peptide(&mut file, count, entry, id);
            }
        });

        TableWriter {
            taxons: TaxonList::from_file(&cli.taxons),
            wrong_ids: HashSet::new(),
            peptides: Some(peptide_sender),
            uniprot_entries: open_write(&cli.uniprot_entries),
            go_cross_references: Some(go_ref_sender),
            ec_cross_references: Some(ec_ref_sender),
            ip_cross_references: Some(ip_ref_sender),

            peptide_count: 0,
            uniprot_count: 0,

            threads: vec![go_handle, ec_handle, ip_handle, peptide_handle],
        }
    }

    // Store a complete entry in the database
    pub fn store(&mut self, entry: Entry) {
        let id = self.add_uniprot_entry(&entry);

        // Failed to add entry
        if id == -1 { return; }

        for r in &entry.go_references {
            if let Some(go) = &self.go_cross_references {
                go.send((id, r.clone())).expect("unable to send message to GO worker thread");
            }
        }

        for r in &entry.ec_references {
            if let Some(ec) = &self.ec_cross_references {
                ec.send((id, r.clone())).expect("unable to send message to EC worker thread");
            }
        }

        for r in &entry.ip_references {
            if let Some(ip) = &self.ip_cross_references {
                ip.send((id, r.clone())).expect("unable to send message to InterPro worker thread");
            }
        }

        if let Some(pept) = &self.peptides {
            pept.send((id, entry)).expect("unable to send message to peptide worker thread");
        }
    }

    pub fn close(&mut self) {
        self.peptides = None;
        self.ip_cross_references = None;
        self.ec_cross_references = None;
        self.go_cross_references = None;
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
}

fn add_go_ref(writer: &mut Writer, count: u64, ref_id: String, uniprot_entry_id: i64) {
    if let Err(e) = writeln!(
        writer,
        "{}\t{}\t{}",
        count, uniprot_entry_id, ref_id
    ) {
        eprintln!("{}\tError adding GO reference to the database.\n{:?}", Instant::now().elapsed().as_millis(), e);
    }
}

fn add_ec_ref(writer: &mut Writer, count: u64, ref_id: String, uniprot_entry_id: i64) {
    if let Err(e) = writeln!(
        writer,
        "{}\t{}\t{}",
        count, uniprot_entry_id, ref_id
    ) {
        eprintln!("{}\tError adding EC reference to the database.\n{:?}", Instant::now().elapsed().as_millis(), e);
    }
}

fn add_ip_ref(writer: &mut Writer, count: u64, ref_id: String, uniprot_entry_id: i64) {
    if let Err(e) = writeln!(
        writer,
        "{}\t{}\t{}",
        count, uniprot_entry_id, ref_id,
    ) {
        eprintln!("{}\tError adding InterPro reference to the database.\n{:?}", Instant::now().elapsed().as_millis(), e);
    }
}

fn process_peptide(writer: &mut Writer, count: u64, entry: Entry, uniprot_entry_id: i64) {
    let digest = entry.digest();
    let go_ids = entry.go_references.into_iter();
    let ec_ids = entry.ec_references.iter().filter(|x| !x.is_empty()).map(|x| format!("EC:{}", x)).into_iter();
    let ip_ids = entry.ip_references.iter().filter(|x| !x.is_empty()).map(|x| format!("IPR:{}", x)).into_iter();

    let summary = go_ids.chain(ec_ids).chain(ip_ids).collect::<Vec<String>>().join(";");

    for sequence in digest {
        add_peptide(writer, count, sequence.replace("I", "L"), uniprot_entry_id, sequence, summary.clone());
    }
}

fn add_peptide(writer: &mut Writer, count: u64, sequence: String, id: i64, original_sequence: String, annotations: String) {
    if let Err(e) = writeln!(
        writer,
        "{}\t{}\t{}\t{}\t{}",
        count,
        sequence, original_sequence,
        id, annotations
    ) {
        eprintln!("{}\tError writing to CSV.\n{:?}", now(), e);
    }
}