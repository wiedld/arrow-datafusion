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

use super::cursor::{Cursor, CursorValues};

/// A representation of a record batch,
/// passable and sliceable through multiple merge nodes
/// in a cascaded merge tree.
///
/// A `BatchCursor` encapsulates the ability to sort merge each
/// sliced portition of a record batch, with minimal overhead.
///
/// ```text
/// ┌────────────────────────┐
/// │ CursorValues   Batch   │           ┌──────────────────────┐
/// │ ┌──────────┐ ┌───────┐ │    ─ ─ ─ ▶│      BatchTracker    │
/// │ │  1..10   │ │   A   │ ┼ ─ │       └──────────────────────┘
/// │ ├──────────┤ ├───────┤ │   │            Holds batches
/// │ │  11..20  │ │   B   │ ┼ ─ ┘          and assigns BatchId
/// │ └──────────┘ └───────┘ │
/// └────────────────────────┘
///             │
///             │
///             ▼
///        BatchCursors
/// ┌────────────────────────┐           ┌──────────────────────┐ ─ ▶ push batch
/// │    Cursor     BatchId  │    ─ ─ ─ ▶│   LoserTree (Merge)  │ ─ ▶ advance cursor
/// │ ┌──────────┐ ┌───────┐ │   │       └──────────────────────┘ ─ ▶ push row
/// │ │  1..10   │ │   A   │ ┼ ─ │       ┌──────────────────────┐         │
/// │ ├──────────┤ ├───────┤ │   │       │   SortOrderBuilder  ◀┼ ─ ─ ─ ─ ┘
/// │ │  11..20  │ │   B   │ ┼ ─ ┘       └──────────────────────┘
/// │ └──────────┘ └───────┘ │                holds sorted rows
/// └────────────────────────┘              up to ceiling size N
///                                                  │
///              ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///             │
///             ▼
///        BatchCursors
/// ┌────────────────────────┐
/// │    Cursor     BatchId  │           ┌──────────────────────┐
/// │ ┌──────────┐ ┌───────┐ │    ─ ─ ─ ▶│   LoserTree (Merge)  │
/// │ │  1..7    │ │   A   │ ┼ ─ │       └──────────│───────────┘
/// │ ├──────────┤ ├───────┤ │   │       ┌──────────▼───────────┐
/// │ │  11..14  │ │   B   │ ┼ ─ │       │   SortOrderBuilder   |
/// │ └──────────┘ └───────┘ │   │       └──────────────────────┘
/// │     ...         ...    │   │
/// │ ┌──────────┐ ┌───────┐ │   │
/// │ │ 101..103 │ │   F   │ ┼ ─ │
/// │ ├──────────┤ ├───────┤ │   │
/// │ │ 151..157 │ │   G   │ ┼ ─ ┘
/// │ └──────────┘ └───────┘ │
/// └────────────────────────┘
///             ▲
///             │
///         Mirror of above.
///  LoserTree (Merge) & SortOrderBuilder
///       yielding BatchCursors
///  which represents partial batches
/// ```
///
///
/// Final merge at CascadedMerge root:
/// ```text
///
///         SortOrder
/// ┌───────────────────────────┐
/// | (B,11) (F,101) (A,1) ...  ┼ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// └───────────────────────────┘                  |
///                                                |
///        BatchTracker                            |
/// ┌────────────────────────┐                     |
/// │    Batch      BatchId  │           ┌─────────▼────────────┐
/// │ ┌──────────┐ ┌───────┐ │    ─ ─ ─ ▶│   CascadedMerge root │
/// │ │  <data>  │ │   A   │ ┼ ─ │       │         |            │
/// │ ├──────────┤ ├───────┤ │   │       |         ▼            |
/// │ │  <data>  │ │   B   │ ┼ ─ │       │     interleave       |
/// │ └──────────┘ └───────┘ │   │       └─────────|────────────┘
/// │     ...         ...    │   │                 |
/// │ ┌──────────┐ ┌───────┐ │   │                 ▼
/// │ │  <data>  │ │   F   │ ┼ ─ │          batch up to N rows
/// │ ├──────────┤ ├───────┤ │   │
/// │ │  <data>  │ │   G   │ ┼ ─ ┘
/// │ └──────────┘ └───────┘ │
/// └────────────────────────┘
///
/// ```
///
#[derive(Debug)]
pub struct BatchCursor<C: CursorValues> {
    /// Unique identifier of a record batch
    batch_id: BatchId,

    /// The cursor for the given batch.
    pub cursor: Cursor<C>,
}

impl<C: CursorValues> BatchCursor<C> {
    /// Create a new [`BatchCursor`] from [`CursorValues`] and a [`BatchId`].
    pub fn new(batch_id: BatchId, cursor_values: C) -> Self {
        Self {
            batch_id,
            cursor: Cursor::new(cursor_values),
        }
    }

    /// Cursor is in progress
    pub fn in_progress(&self) -> bool {
        self.cursor.current_index() > 0 && !self.cursor.is_finished()
    }

    /// Reset for next merge node
    pub fn reset(&mut self) {
        let cursor_values = self.cursor.cursor_values();
        let cursor_values = cursor_values.slice(0, cursor_values.len());
        std::mem::swap(&mut self.cursor, &mut Cursor::new(cursor_values));
    }

    /// Slice for partial yielded batches
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let cursor = Cursor::new(self.cursor.cursor_values().slice(offset, length));
        Self {
            batch_id: self.batch_id,
            cursor,
        }
    }

    /// Get batch_id
    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }
}

/// Unique tracking id, assigned per record batch.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct BatchId(pub u64);

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
        let batch_id = BatchId(self.monotonic_counter.fetch_add(1, Ordering::Relaxed));
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
