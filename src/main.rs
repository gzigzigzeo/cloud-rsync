use argh::FromArgs;
use console::style;
use humansize::{format_size, DECIMAL};
use indicatif::ProgressIterator;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::time::Instant;

use crate::signature::{Diff, Op, Signature};

mod blake3_serde_hex;
mod builder;
mod progress_bar;
mod signature;

const SIG_EXT: &str = ".rsig";

trait Runner {
    fn run(&self) -> Result<(), Box<dyn Error>>;
}

#[derive(FromArgs, PartialEq, Debug)]
/// zsync for GCS
struct CLI {
    #[argh(subcommand)]
    command: Command,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum Command {
    Sign(SignCommand),
    Diff(DiffCommand),
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "sign")]
/// Generate file signature
struct SignCommand {
    /// file mask (ex: "*.psd")
    #[argh(positional)]
    mask: String,

    /// min chunk size
    #[argh(option, default = "4096")]
    min_size: u32,

    /// avg chunk size
    #[argh(option, default = "16384")]
    avg_size: u32,

    /// max chunk size
    #[argh(option, default = "65536")]
    max_size: u32,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "diff")]
/// Generate diff between two signatures and print the stats
struct DiffCommand {
    /// source file path
    #[argh(positional)]
    source: String,

    /// target file path
    #[argh(positional)]
    target: String,

    /// keep diff file
    #[argh(option, default = "true")]
    keep_diff_file: bool,
}

impl Runner for Command {
    fn run(&self) -> Result<(), Box<dyn Error>> {
        match &self {
            Self::Sign(sign) => sign.run(),
            Self::Diff(diff) => diff.run(),
        }
    }
}

impl Runner for SignCommand {
    fn run(&self) -> Result<(), Box<dyn Error>> {
        println!("Calculating signatures for {}:", &self.mask);
        println!();

        let total_start = Instant::now();

        for source_dir_entry in globwalk::glob(&self.mask)? {
            let source_dir_entry = source_dir_entry?;
            let source_path = source_dir_entry.path();
            let source_path_str = match source_path.to_str() {
                Some(path) => path,
                _ => return Err("Source file path is empty".into()),
            };

            if source_path.is_dir() {
                continue;
            }

            let target_path = String::from(source_path_str) + SIG_EXT;

            let source_file = File::open(source_path)?;
            let mut reader = BufReader::new(source_file);

            let spinner = progress_bar::create_spinner(format!(
                "Calculating signature for {:?}...",
                source_path_str
            ));

            let start = Instant::now();

            let sig = signature::Signature::generate(
                &mut reader,
                self.min_size,
                self.avg_size,
                self.max_size,
            )?;

            let serialized = serde_json::to_string_pretty(&sig)?;

            let mut output_file = File::create(&target_path)?;
            output_file.write_all(serialized.as_bytes())?;

            spinner.finish_with_message(format!(
                "Took {:.2?}, source file size: {}, saved to: {:}",
                start.elapsed(),
                format_size(sig.length(), DECIMAL),
                target_path
            ));
        }

        println!();
        println!(
            "{}",
            style(format!("Done in {:.2?}!", total_start.elapsed())).green()
        );

        Ok(())
    }
}

impl Runner for DiffCommand {
    fn run(&self) -> Result<(), Box<dyn Error>> {
        println!("Calculating diff for {} .. {}:", self.source, self.target);
        println!();

        let total_start = Instant::now();

        let source_sig_file = File::open(&self.source)?;
        let target_sig_file = File::open(&self.target)?;

        let source_sig: Signature = serde_json::from_reader(source_sig_file)?;
        let target_sig: Signature = serde_json::from_reader(target_sig_file)?;

        let diff = match Diff::new(&source_sig, &target_sig) {
            Some(diff) => diff,
            None => {
                println!("{}", style("Files are equal!").green());
                return Ok(());
            }
        };

        println!(
            "Source file size: {} ({} bytes)",
            format_size(source_sig.length(), DECIMAL),
            source_sig.length()
        );

        println!(
            "Target file size: {} ({} bytes)",
            format_size(target_sig.length(), DECIMAL),
            target_sig.length()
        );

        let len_diff = (target_sig.length() as i64 - source_sig.length() as i64).abs() as usize;

        println!(
            "Difference: {} ({} bytes)",
            format_size(len_diff, DECIMAL),
            len_diff
        );

        println!();
        println!(
            "{} COPY ops from the old file: {} ({} bytes)",
            diff.copy_ops().len(),
            format_size(diff.copy_length(), DECIMAL),
            diff.copy_length()
        );

        println!(
            "{} INSERT to the new file: {} ({} bytes)",
            diff.insert_ops().len(),
            format_size(diff.insert_length(), DECIMAL),
            diff.insert_length()
        );

        println!();
        println!("Ranges to request & insert:");
        println!();

        for (index, op) in diff.insert_ops().iter().enumerate() {
            println!(
                "{:<4} [ {:<12}: {:<12} ]",
                format!("{})", index + 1),
                op.offset(),
                op.length()
            )
        }

        println!();

        let (source_file_name, _) = self.source.split_at(self.source.len() - SIG_EXT.len());
        let (target_file_name, _) = self.target.split_at(self.target.len() - SIG_EXT.len());
        let destination_file_name = String::from(target_file_name) + ".NEW";

        let mut source_file = File::open(source_file_name)?;
        let mut target_file = File::open(target_file_name)?;
        let mut diff_file = tempfile::NamedTempFile::new()?;
        let mut dst_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&destination_file_name)?;

        println!(
            "Building {} temporary file...",
            diff_file.path().to_str().unwrap()
        );

        let diff_pbar = progress_bar::create_bar(diff.insert_ops().len() as u64);

        // target_file can be a wrapper over Read which does HTTP queries to GCS.
        // Or, this wrapper may collect the read+seek calls and do actual queries later.
        // Or, this method may be used in a middleware service to generate a diff file.
        let diff_schema = builder::build_local_diff_file(
            &mut target_file,
            &mut diff_file,
            diff.insert_ops().iter().progress_with(diff_pbar),
        )?;

        println!(
            "Built {} segments in the temporary diff file.",
            diff_schema.len()
        );

        let build_pbar = progress_bar::create_bar(diff.insert_ops().len() as u64);

        // Builds local file
        builder::build_local_file(
            &mut source_file,
            &mut dst_file,
            diff.operations().iter().progress_with(build_pbar),
            diff_file.as_file_mut(),
            &diff_schema,
        )?;

        if self.keep_diff_file {
            diff_file.keep()?;
        }

        println!();
        println!("Written the new file: {}", &destination_file_name);

        println!();
        println!(
            "{}",
            style(format!("Done in {:.2?}!", total_start.elapsed())).green()
        );

        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli: CLI = argh::from_env();
    cli.command.run()
}
