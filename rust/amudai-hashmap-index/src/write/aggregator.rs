use crate::write::builder::get_entry_bucket_index;
use amudai_collections::identity_hash::IdentityHashMap;
use amudai_common::Result;
use amudai_io::temp_file_store::{TemporaryFileStore, TemporaryWritable};
use rayon::prelude::*;
use std::{
    io::{BufReader, BufWriter, Read, Write},
    sync::Arc,
};

/// Uses temporary files to store hash-position entries, making it suitable for
/// large datasets that cannot fit in memory. Supports parallel processing
/// through multiple temporary files and bucket-based organization.
pub struct EntriesAggregator {
    temp_store: Arc<dyn TemporaryFileStore>,
    temp_files: Vec<BufWriter<Box<dyn TemporaryWritable>>>,
    max_memory_usage: usize,
}

impl EntriesAggregator {
    /// Creates a new disk-based entries aggregator and sorter.
    ///
    /// The aggregator uses temporary files to sort entries. The number of files
    /// is determined by the available parallelism of the system (usually, the
    /// number of CPU cores).
    ///
    /// # Parameters
    ///
    /// * `temp_store` - The temporary file store to use for creating temporary files.
    ///                  
    /// * `max_memory_usage` - The maximum memory usage allowed for the aggregator. This is
    ///   a recommendation that affects the number of sorters running
    ///   in parallel. It's still possible that a single sorter uses
    ///   more memory than this limit.
    ///
    /// # Errors
    /// Returns an error if temporary file allocation fails.
    pub fn new(temp_store: Arc<dyn TemporaryFileStore>, max_memory_usage: usize) -> Result<Self> {
        let num_files = std::thread::available_parallelism()?
            .get()
            .next_power_of_two()
            .max(2);
        let temp_files = (0..num_files)
            .map(|_| {
                temp_store
                    .allocate_writable(None)
                    .map_err(|e| e.into())
                    .map(BufWriter::new)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(EntriesAggregator {
            temp_store,
            temp_files,
            max_memory_usage,
        })
    }

    /// Adds a new hash-position entry to the aggregator.
    ///
    /// # Parameters
    /// * `hash` - The hash value of the entry
    /// * `position` - The position value associated with this hash
    ///
    /// # Returns
    /// Returns `Ok(())` on success, or an error if the operation fails.
    pub fn push_entry(&mut self, hash: u64, position: u64) -> Result<()> {
        let bucket_index = get_entry_bucket_index(hash, self.temp_files.len());
        let temp_file = &mut self.temp_files[bucket_index];
        temp_file.write_all(&hash.to_le_bytes())?;
        temp_file.write_all(&position.to_le_bytes())?;
        Ok(())
    }

    /// Completes the aggregation process and returns the count of unique entries.
    ///
    /// This method finalizes the aggregation, potentially performing deduplication
    /// and other cleanup operations.
    ///
    /// # Returns
    /// Returns the number of unique hash entries that were aggregated.
    pub fn finish(&mut self) -> Result<usize> {
        let temp_files = std::mem::take(&mut self.temp_files);
        let aggregated_results = temp_files
            .into_par_iter()
            .map(|temp_file| self.aggregate_entries(temp_file))
            .collect::<Result<Vec<_>>>()?;

        let unique_count = aggregated_results
            .iter()
            .map(|(_, count)| count)
            .sum::<usize>();

        self.temp_files = aggregated_results
            .into_iter()
            .map(|(temp_file, _)| temp_file)
            .collect::<Vec<_>>();

        Ok(unique_count)
    }

    /// Produces a sorted iterator over the aggregated entries.
    ///
    /// The entries are sorted according to their bucket assignments based on
    /// the provided bucket count.
    ///
    /// # Parameters
    /// * `buckets_count` - The number of buckets to use for sorting
    ///
    /// # Returns
    /// Returns a struct `SortedEntries` for iterating over sorted entries.
    pub fn sorted_entries(&mut self, buckets_count: usize) -> Result<SortedEntries> {
        let mut temp_files = std::mem::take(&mut self.temp_files)
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();

        // Estimate the memory usage per file based on the current size of each temporary file.
        let estimated_mem_usage_per_file = temp_files
            .iter()
            .map(|temp_file| temp_file.as_ref().unwrap().get_ref().current_size() * 2)
            .max()
            .unwrap() as usize;

        let allowed_parallelism = (self.max_memory_usage / estimated_mem_usage_per_file)
            .max(1)
            .min(temp_files.len());

        // Sort the aggregated entries in parallel, using the allowed parallelism
        // to limit the number of concurrent sort operations.
        let temp_files = temp_files
            .chunks_mut(allowed_parallelism)
            .flat_map(|chunk| {
                chunk
                    .into_par_iter()
                    .map(|temp_file| {
                        self.sort_aggregated_entries(temp_file.take().unwrap(), buckets_count)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Result<Vec<_>>>()?;

        SortedEntries::new(temp_files, buckets_count)
    }

    /// Reads the dumped entries from a temporary file, aggregates position lists by hash,
    /// and returns a new temporary file with the aggregated entries.
    fn aggregate_entries(
        &self,
        mut temp_file: BufWriter<Box<dyn TemporaryWritable>>,
    ) -> Result<(BufWriter<Box<dyn TemporaryWritable>>, usize)> {
        temp_file.flush()?;

        let mut entry_map = IdentityHashMap::default();
        {
            let mut reader = BufReader::new(
                temp_file
                    .into_inner()
                    .map_err(|e| e.into_error())?
                    .into_reader()?,
            );
            let mut buffer = [0u8; 16];
            loop {
                match reader.read_exact(&mut buffer) {
                    Ok(_) => {
                        let hash = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
                        let position = u64::from_le_bytes(buffer[8..16].try_into().unwrap());
                        entry_map
                            .entry(hash)
                            .or_insert_with(Vec::new)
                            .push(position);
                    }
                    Err(err) => match err.kind() {
                        std::io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(err.into()),
                    },
                }
            }
        }

        let entries_count = entry_map.len();
        let mut temp_file = BufWriter::new(self.temp_store.allocate_writable(None)?);
        for (hash, positions) in entry_map {
            let positions_slice = positions.as_slice();
            if positions_slice.is_empty() {
                continue;
            }
            temp_file.write_all(&hash.to_le_bytes())?;
            temp_file.write_all(&(positions_slice.len() as u64).to_le_bytes())?;
            for &position in positions_slice {
                temp_file.write_all(&position.to_le_bytes())?;
            }
        }
        temp_file.flush()?;
        Ok((temp_file, entries_count))
    }

    /// Loads the aggregated entries from a temporary file, sorts them by bucket index determined
    /// by the hash, and writes them to a new temporary file.
    fn sort_aggregated_entries(
        &self,
        temp_file: BufWriter<Box<dyn TemporaryWritable>>,
        buckets_count: usize,
    ) -> Result<Box<dyn TemporaryWritable>> {
        let mut sorted_entries = Vec::with_capacity(buckets_count);
        {
            let mut reader = BufReader::new(
                temp_file
                    .into_inner()
                    .map_err(|e| e.into_error())?
                    .into_reader()?,
            );
            let mut buffer = [0u8; 16];
            loop {
                match reader.read_exact(&mut buffer) {
                    Ok(_) => {
                        let hash = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
                        let positions_count =
                            u64::from_le_bytes(buffer[8..16].try_into().unwrap()) as usize;
                        let mut positions_buf = vec![0; positions_count * 8];
                        reader.read_exact(&mut positions_buf)?;
                        sorted_entries.push((hash, positions_buf));
                    }
                    Err(err) => match err.kind() {
                        std::io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(err.into()),
                    },
                }
            }
        }

        sorted_entries
            .par_sort_unstable_by_key(|(hash, _)| get_entry_bucket_index(*hash, buckets_count));

        let temp_file = self.temp_store.allocate_writable(None)?;
        let mut writer = BufWriter::new(temp_file);
        for (hash, positions) in sorted_entries {
            writer.write_all(&hash.to_le_bytes())?;
            let positions_count = positions.len() as u64 / 8;
            writer.write_all(&positions_count.to_le_bytes())?;
            writer.write_all(&positions)?;
        }
        writer.flush()?;
        writer.into_inner().map_err(|e| e.into_error().into())
    }
}

/// Manages multiple file readers and merges their sorted content to provide
/// a unified sorted iterator over hash-position entries. Uses a k-way merge
/// algorithm to efficiently combine entries from multiple sorted temporary files.
pub struct SortedEntries {
    readers: Vec<BufReader<Box<dyn Read>>>,
    read_values: Vec<Option<(u64, Vec<u64>)>>,
    buckets_count: usize,
}

impl SortedEntries {
    /// Creates a new disk-based sorted entries iterator.
    ///
    /// Initializes readers for all temporary files and pre-loads the first
    /// value from each reader to prepare for the k-way merge process.
    ///
    /// # Parameters
    /// * `temp_files` - Vector of temporary files containing sorted entries
    /// * `buckets_count` - Number of buckets used for determining sort order
    ///
    /// # Returns
    /// Returns a new instance ready for iteration over sorted entries.
    ///
    /// # Errors
    /// Returns an error if file readers cannot be created or initial values
    /// cannot be read from the files.
    pub fn new(temp_files: Vec<Box<dyn TemporaryWritable>>, buckets_count: usize) -> Result<Self> {
        let mut readers = temp_files
            .into_iter()
            .map(|temp_file| Ok(BufReader::new(temp_file.into_reader()?)))
            .collect::<Result<Vec<_>>>()?;

        let read_values = readers
            .iter_mut()
            .map(Self::read_value)
            .collect::<Result<Vec<_>>>()?;

        Ok(SortedEntries {
            readers,
            read_values,
            buckets_count,
        })
    }

    fn read_value(reader: &mut BufReader<Box<dyn Read>>) -> Result<Option<(u64, Vec<u64>)>> {
        let mut buffer = [0u8; 16];
        match reader.read_exact(&mut buffer) {
            Ok(_) => {
                let hash = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
                let positions_count =
                    u64::from_le_bytes(buffer[8..16].try_into().unwrap()) as usize;

                let mut positions = vec![0u64; positions_count];
                for position in positions.iter_mut() {
                    let mut pos_buf = [0u8; 8];
                    reader.read_exact(&mut pos_buf)?;
                    *position = u64::from_le_bytes(pos_buf);
                }
                Ok(Some((hash, positions)))
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => Ok(None),
                _ => Err(err.into()),
            },
        }
    }

    /// Retrieves the next entry from the sorted sequence.
    ///
    /// # Returns
    /// Returns `Some((hash, positions))` if an entry is available,
    /// or `None` if the sequence is exhausted. The positions vector contains all
    /// positions associated with the given hash.
    pub fn next_entry(&mut self) -> Result<Option<(u64, Vec<u64>)>> {
        let min_value = self
            .read_values
            .iter_mut()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by_key(|(_, v)| get_entry_bucket_index(v.as_ref().unwrap().0, self.buckets_count));

        if let Some((reader_idx, v)) = min_value {
            let v = v.take();
            self.read_values[reader_idx] = Self::read_value(&mut self.readers[reader_idx])?;
            return Ok(v);
        }
        Ok(None)
    }
}
