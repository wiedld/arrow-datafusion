// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use ahash::RandomState;
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;

use std::collections::{HashMap, hash_map::Entry};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Debug, Copy, Clone, Default)]
struct BatchCursor {
    /// The index into BatchBuilder::batches
    batch_id: u64,
    /// The row index within the given batch
    row_idx: usize,
}

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
#[derive(Debug)]
pub struct BatchBuilder {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Accounts for memory used by buffered batches
    reservation: MemoryReservation,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<BatchCursor>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_id, row_idx)`
    indices: Vec<(u64, usize)>,

    /// Monotonically increasing batch id
    monotonic_counter: AtomicU64,

    /// Hold in hashmap, avoiding the Vec::retain.
    batches: HashMap<u64, Arc<RecordBatch>, RandomState>,
}

impl BatchBuilder {
    /// Create a new [`BatchBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(
        schema: SchemaRef,
        stream_count: usize,
        batch_size: usize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            schema,
            monotonic_counter: AtomicU64::new(0),
            batches: HashMap::with_hasher(RandomState::new()),
            cursors: vec![BatchCursor::default(); stream_count],
            indices: Vec::with_capacity(batch_size),
            reservation,
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(&mut self, stream_idx: usize, batch: RecordBatch) -> Result<()> {
        self.reservation.try_grow(batch.get_array_memory_size())?;
        let batch_id = self.monotonic_counter.fetch_add(1, Ordering::Relaxed);
        self.batches.insert(batch_id, Arc::new(batch));
        self.cursors[stream_idx] = BatchCursor {
            batch_id,
            row_idx: 0,
        };
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        let cursor = &mut self.cursors[stream_idx];
        let row_idx = cursor.row_idx;
        cursor.row_idx += 1;
        self.indices.push((cursor.batch_id, row_idx));
    }

    /// Returns the number of in-progress rows in this [`BatchBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`BatchBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the schema of this [`BatchBuilder`]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    ///
    /// Returns `None` if no pending rows
    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        let mut batches_to_interleave = Vec::with_capacity(self.indices.len());
        let mut batches_seen: HashMap<u64, (usize, usize)> =
            HashMap::with_capacity(self.indices.len()); // (batch_idx, max_row_idx)

        let mut adjusted_indices = Vec::with_capacity(self.indices.len());

        for (batch_id, row_idx) in self.indices.iter() {
            let batch_idx = match batches_seen.entry(*batch_id) {
                Entry::Occupied(entry) => entry.get().0,
                Entry::Vacant(entry) => {
                    let batch_idx = batches_to_interleave.len();
                    batches_to_interleave
                        .push(self.batches.get(batch_id).expect("should exist").clone());
                    entry.insert((batch_idx, *row_idx));
                    batch_idx
                }
            };
            adjusted_indices.push((batch_idx, *row_idx));
        }

        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = batches_to_interleave
                    .iter()
                    .map(|batch| batch.column(column_idx).as_ref())
                    .collect();
                Ok(interleave(&arrays, adjusted_indices.as_slice())?)
            })
            .collect::<Result<Vec<_>>>()?;

        self.indices.clear();

        // Drop all fully consumed batches
        for (batch_id, (_, max_row_idx)) in batches_seen.iter() {
            let batch = self.batches.get(batch_id).expect("should exist");
            if batch.num_rows() == max_row_idx + 1 {
                let free_bytes = batch.get_array_memory_size();
                self.batches.remove(batch_id);
                self.reservation.shrink(free_bytes);
            }
        }

        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}
