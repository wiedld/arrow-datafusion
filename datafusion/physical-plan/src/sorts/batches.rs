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
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub type BatchId = u64;

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct BatchCursor<C> {
    /// Unique identifier for the batch
    pub batch: BatchId,

    /// When slicing cursors, the starting_offset provides the mapping of
    /// ```Batch[starting_offset] == SlicedCursor[0]```
    starting_offset: usize,

    /// The row index within the given batch
    pub row_idx: usize,

    /// The cursor for the given batch.
    pub cursor: C,
}

impl<C> BatchCursor<C> {
    /// Create a new [`BatchCursor`] from a [`Cursor`](super::cursor::Cursor) and a [`BatchId`].
    pub fn new(batch: BatchId, cursor: C) -> Self {
        Self {
            batch,
            cursor,
            row_idx: 0,
            starting_offset: 0,
        }
    }

    /// Reset for next merge node sorting
    pub fn reset(&mut self) {
        self.row_idx = self.starting_offset;
    }

    /// Denote BatchCursor is in progress of being sorted.
    pub fn in_progress(&self) -> bool {
        self.row_idx > 0
    }
}

/// For storing the record batches outside of the cascading merge tree.
pub struct BatchTracker {
    /// Monotonically increasing batch id
    monotonic_counter: AtomicU64,
    /// Write once, read many [`RecordBatch`]s
    batches: Mutex<HashMap<BatchId, Arc<RecordBatch>, RandomState>>,
    /// Accounts for memory used by buffered batches
    reservation: Mutex<MemoryReservation>,
}

impl BatchTracker {
    pub fn new(reservation: MemoryReservation) -> Self {
        Self {
            monotonic_counter: AtomicU64::new(0),
            batches: Mutex::new(HashMap::with_hasher(RandomState::new())),
            reservation: Mutex::new(reservation),
        }
    }

    pub fn add_batch(&self, batch: RecordBatch) -> Result<BatchId> {
        self.reservation
            .lock()
            .try_grow(batch.get_array_memory_size())?;
        let batch_id = self.monotonic_counter.fetch_add(1, Ordering::Relaxed);
        self.batches.lock().insert(batch_id, Arc::new(batch));
        Ok(batch_id)
    }

    pub fn get_batches(&self, batch_ids: &[BatchId]) -> Vec<Arc<RecordBatch>> {
        let batches = self.batches.lock();
        batch_ids.iter().map(|id| batches[id].clone()).collect()
    }

    pub fn remove_batches(&self, batch_ids: &[BatchId]) {
        let mut batches = self.batches.lock();
        for id in batch_ids {
            batches.remove(id);
        }
    }
}

impl std::fmt::Debug for BatchTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchTracker").finish()
    }
}
