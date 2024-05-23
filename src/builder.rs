use crate::signature::{InsertOp, Op, Operation};
use std::collections::HashMap;
use std::error::Error;
use std::io::{copy, Read, Seek, SeekFrom, Write};

#[derive(Debug, Clone, Copy)]
pub struct Segment {
    at: u64,
    length: usize,
}

pub type DiffSchema = HashMap<uuid::Uuid, Segment>;

/// Builds local temporary file with segments for InsertOp.
///
/// # Parameters:
/// - `r`: source stream
/// - `w`: destination stream
/// - `ops`: InsertOp iterator
///
/// # Returns:
/// - `Result<Segments, Box<dyn Error>>` where Segment represents a segment for InsertOp.
pub fn build_local_diff_file<'a, R, W, I>(
    r: &mut R,
    w: &mut W,
    ops: I,
) -> Result<DiffSchema, Box<dyn Error>>
where
    R: Read + Seek,
    W: Write,
    I: IntoIterator<Item = &'a InsertOp>,
{
    let mut segments: DiffSchema = DiffSchema::new();

    let mut at: u64 = 0;

    for op in ops {
        let offset = op.offset();
        let length = op.length();

        r.seek(SeekFrom::Start(offset))?;
        let mut chunk = r.take(length as u64);
        copy(&mut chunk, w)?;

        segments.insert(op.uuid(), Segment { at, length });

        at += length as u64;
    }

    Ok(segments)
}

/// Builds destination file from source and diff file.
pub fn build_local_file<'a, R, W, I>(
    source: &mut R,
    destination: &mut W,
    ops: I,
    diff_file: &mut R,
    diff_schema: &DiffSchema,
) -> Result<(), Box<dyn Error>>
where
    R: Read + Seek,
    W: Write,
    I: IntoIterator<Item = &'a Operation>,
{
    for op in ops {
        match op {
            Operation::COPY(cp) => {
                source.seek(SeekFrom::Start(cp.source_offset() as u64))?;
                let mut chunk = source.take(cp.length() as u64);
                copy(&mut chunk, destination)?;
            }
            Operation::INSERT(ins) => {
                let segment = match diff_schema.get(&ins.uuid()) {
                    Some(s) => s,
                    None => return Err(format!("Can not find segment {}", ins.uuid()).into()),
                };

                diff_file.seek(SeekFrom::Start(segment.at))?;
                let mut chunk = diff_file.take(segment.length as u64);
                copy(&mut chunk, destination)?;
            }
        }
    }

    Ok(())
}
