use fastcdc::v2020::StreamCDC;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::io::Read;

use crate::blake3_serde_hex;

/// TODO:
///
/// I think, it worth trying to merge CopyOp and InsertOp into a single struct.
/// This struct would have: kind, target_offset, source_offset, length, uuid.
/// InsertOp would have both offsets the same.
///
/// It may make things simpler.

/// Represents the chunk of a file
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Chunk {
    length: usize,
    offset: u64,

    #[serde(with = "blake3_serde_hex")]
    strong_hash: blake3::Hash,
}

/// Represents the signature for a file
#[derive(Debug, Serialize, Deserialize)]
pub struct Signature {
    #[serde(with = "blake3_serde_hex")]
    strong_hash: blake3::Hash,
    length: usize,
    chunks: Vec<Chunk>,
}

/// CopyOp represents COPY operation for a target diff.
/// COPY takes the segment of a source file and copies
/// it to a destination file.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct CopyOp {
    //// offset in the source file (used for download/copy)
    source_offset: u64,

    /// offset in the target file (used for sorting target file segments)
    offset: u64,

    /// length of the segment
    length: usize,
}

/// InsertOp represents INSERT operation for a target diff.
/// INSERT takes the segment of a target file and copies it
/// to a destination file.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct InsertOp {
    /// offset in the target file
    offset: u64,

    /// length of the segment
    length: usize,

    /// id used to navigate diff file
    uuid: uuid::Uuid,
}

/// Represents an INSERT or COPY operation in a sequential list
#[derive(Debug, PartialEq, Eq)]
pub enum Operation {
    INSERT(InsertOp),
    COPY(CopyOp),
}

/// Represents difference between two files.
#[derive(Debug)]
pub struct Diff {
    insert_ops: Vec<InsertOp>,
    copy_ops: Vec<CopyOp>,
    copy_length: usize,
    insert_length: usize,
    operations: Vec<Operation>,
}

impl PartialEq for Signature {
    // No need to compare all chunks if both signatures are equal
    fn eq(&self, other: &Signature) -> bool {
        self.strong_hash == other.strong_hash
    }
}

impl Eq for Signature {}

/// Represents op which can be chained
trait ChainableOp {
    /// Returns true if the current chunk precedes the given chunk
    /// in both source and target files. Such chanks can be joined
    /// together into a longer chunk.
    ///
    /// # Parameters
    ///
    /// - `self`: a reference to current chunk
    /// - `other`: a reference to another chunk.
    ///
    /// # Returns
    ///
    /// - `bool`: If the current chunk precedes the given chunk.
    fn can_chain(&self, other: &Self) -> bool;

    /// Extends the current chunk by a length of a given chunk
    /// joining them togeter.
    ///
    /// # Parameters:
    ///
    /// - `length`: length to extend the current chunk by.
    fn chain(&mut self, length: usize);
}

pub trait Op {
    /// Returns offset of current op in the target file
    ///
    /// # Returns:
    /// - `u64: offset
    fn offset(&self) -> u64;

    /// Returns length of current op
    ///
    /// # Returns:
    /// - `usize`: chunk size
    fn length(&self) -> usize;
}

impl ChainableOp for CopyOp {
    fn can_chain(&self, other: &Self) -> bool {
        self.source_offset + (self.length as u64) == other.source_offset
            && self.offset + (self.length as u64) == other.offset
    }

    fn chain(&mut self, length: usize) {
        self.length = self.length + length
    }
}

impl ChainableOp for InsertOp {
    fn can_chain(&self, other: &Self) -> bool {
        self.offset + (self.length as u64) == other.offset
    }

    fn chain(&mut self, length: usize) {
        self.length = self.length + length
    }
}

impl Op for CopyOp {
    fn offset(&self) -> u64 {
        self.offset
    }

    fn length(&self) -> usize {
        self.length
    }
}

impl Op for InsertOp {
    fn offset(&self) -> u64 {
        self.offset
    }

    fn length(&self) -> usize {
        self.length
    }
}

impl CopyOp {
    pub fn source_offset(&self) -> u64 {
        self.source_offset
    }
}

impl InsertOp {
    pub fn uuid(&self) -> uuid::Uuid {
        self.uuid
    }
}

impl From<InsertOp> for Operation {
    fn from(op: InsertOp) -> Self {
        Self::INSERT(op)
    }
}

impl From<CopyOp> for Operation {
    fn from(op: CopyOp) -> Self {
        Self::COPY(op)
    }
}

impl Operation {
    pub fn offset(&self) -> u64 {
        match self {
            Self::COPY(op) => op.offset(),
            Self::INSERT(op) => op.offset(),
        }
    }
}

impl PartialOrd for Operation {
    fn partial_cmp(&self, other: &Operation) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Operation {
    fn cmp(&self, other: &Operation) -> Ordering {
        self.offset().cmp(&other.offset())
    }
}

impl Signature {
    /// Generates file signature. Uses `fastcdc` to split file into chunks.
    /// Calculates blake3 strong hash for each chunk.
    ///
    /// # Parameters:
    ///
    /// - `reader`: source file reader
    /// - `min_size`: minimum chunk size in bytes
    /// - `avg_size`: average chunk size in bytes
    /// - `max_size`: maximum chunk size in bytes
    ///
    /// # Returns:
    /// - `Result<Self, Box<dyn Error>>`: signature for a file or error
    pub fn generate(
        reader: &mut dyn Read,
        min_size: u32,
        avg_size: u32,
        max_size: u32,
    ) -> Result<Self, Box<dyn Error>> {
        let mut hasher = blake3::Hasher::new();
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut length: usize = 0;

        let chunker = StreamCDC::new(reader, min_size, avg_size, max_size);
        for source_chunk in chunker {
            let source_chunk = source_chunk?;
            hasher.update(&source_chunk.data);

            let strong_hash = blake3::hash(&source_chunk.data);
            let chunk = Chunk {
                length: source_chunk.length,
                offset: source_chunk.offset,
                strong_hash,
            };

            length += chunk.length;

            chunks.push(chunk);
        }

        let strong_hash = hasher.finalize();

        Ok(Self {
            strong_hash,
            chunks,
            length,
        })
    }

    /// Returns a map of chunks by strong hash
    pub(crate) fn chunks_map(&self) -> HashMap<blake3::Hash, &Chunk> {
        let mut m = HashMap::<blake3::Hash, &Chunk>::new();

        for chunk in &self.chunks {
            m.entry(chunk.strong_hash).or_insert(&chunk);
        }

        m
    }

    /// Returns total length of a file.
    pub fn length(&self) -> usize {
        self.length
    }
}

impl Diff {
    pub fn new(source: &Signature, target: &Signature) -> Option<Self> {
        if source == target {
            return None;
        }

        let mut copy_ops: Vec<CopyOp> = Vec::new();
        let mut insert_ops: Vec<InsertOp> = Vec::new();
        let mut copy_length: usize = 0;
        let mut insert_length: usize = 0;
        let mut operations: Vec<Operation> = Vec::new();

        let source_map = source.chunks_map();

        for target_chunk in target.chunks.iter() {
            // If we have a chunk in the source file - use it
            if let Some(source_chunk) = source_map.get(&target_chunk.strong_hash) {
                let op = Self::create_copy_op(source_chunk, target_chunk, &mut copy_ops);
                copy_length += op.length();
            } else {
                let op = Self::create_insert_op(target_chunk, &mut insert_ops);
                insert_length += op.length();
            }
        }

        for op in &copy_ops {
            operations.push(op.clone().into());
        }

        for op in &insert_ops {
            operations.push(op.clone().into());
        }

        operations.sort();

        Some(Self {
            operations,
            copy_length,
            insert_length,
            copy_ops,
            insert_ops,
        })
    }

    /// Creates new CopyOp from source and target chunks. Adds it to copy_ops
    /// vec or extends last copy_op if copies are sequential.
    ///
    /// # Parameters:
    /// - `source_chunk`: Chunk of a source file
    /// - `target_chunk`: Chunk of a target file
    /// - `ops`: Borrowed reference to a copy ops array.
    ///
    /// # Returns:
    /// - `usize`: Length of the created chunk.
    fn create_copy_op(source_chunk: &Chunk, target_chunk: &Chunk, ops: &mut Vec<CopyOp>) -> CopyOp {
        let length = source_chunk.length;

        let op = CopyOp {
            source_offset: source_chunk.offset,
            offset: target_chunk.offset,
            length,
        };

        Self::chain_or_push(op, ops);

        op
    }

    fn create_insert_op(target_chunk: &Chunk, ops: &mut Vec<InsertOp>) -> InsertOp {
        let length = target_chunk.length;
        let uuid = uuid::Uuid::new_v4();

        let op = InsertOp {
            offset: target_chunk.offset,
            length,
            uuid,
        };

        Self::chain_or_push(op, ops);

        op
    }

    fn chain_or_push<T>(op: T, ops: &mut Vec<T>)
    where
        T: ChainableOp + Op,
    {
        if let Some(last_op) = ops.last_mut() {
            if last_op.can_chain(&op) {
                last_op.chain(op.length())
            } else {
                ops.push(op);
            }
        } else {
            ops.push(op);
        }
    }

    pub fn copy_length(&self) -> usize {
        self.copy_length
    }

    pub fn insert_length(&self) -> usize {
        self.insert_length
    }

    pub fn copy_ops(&self) -> &Vec<CopyOp> {
        &self.copy_ops
    }

    pub fn insert_ops(&self) -> &Vec<InsertOp> {
        &self.insert_ops
    }

    pub fn operations(&self) -> &Vec<Operation> {
        &self.operations
    }
}
