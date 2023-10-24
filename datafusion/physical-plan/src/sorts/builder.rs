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

use datafusion_common::Result;

use super::batches::{BatchId, BatchRowSet};
use super::cursor::CursorValues;

pub type YieldedSortOrder<C> = (Vec<BatchRowSet<C>>, Vec<SortOrder>);
pub type SortOrder = (BatchId, usize); // (batch_id, row_idx)

/// Provides an API to incrementally build a [`SortOrder`] from partitioned [`RecordBatch`](arrow::record_batch::RecordBatch)es
#[derive(Debug)]
pub struct SortOrderBuilder<C: CursorValues> {
    /// The current [`BatchRowSet`] for each stream
    active_rowsets: Vec<Option<BatchRowSet<C>>>,

    /// Maintain a list of sorted [`BatchRowSet`]s
    sorted_rowsets: Vec<BatchRowSet<C>>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_id, row_idx)`
    indices: Vec<(BatchId, usize)>,
}

impl<C: CursorValues> SortOrderBuilder<C> {
    /// Create a new [`SortOrderBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(stream_count: usize, batch_size: usize) -> Self {
        Self {
            sorted_rowsets: Vec::with_capacity(stream_count * 2),
            active_rowsets: (0..stream_count).map(|_| None).collect(),
            indices: Vec::with_capacity(batch_size),
        }
    }

    /// Add a new rowset to the active `stream_idx`
    pub fn push_batch(
        &mut self,
        stream_idx: usize,
        rowset: BatchRowSet<C>,
    ) -> Result<()> {
        self.active_rowsets[stream_idx] = Some(rowset);
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        let rowset = self.active_rowsets[stream_idx]
            .as_ref()
            .expect("push row on existing cursor");

        // The loser tree represents 1 node taken up by all ongoing sorts (min heap)
        // plus an extra node at the top for the winner. Hence -1 to get winner's idx.
        let row_idx = rowset.cursor.current_index() - 1;
        self.indices.push((rowset.batch_id(), row_idx));
    }

    /// Returns the number of in-progress rows in this [`SortOrderBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`SortOrderBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Advance the cursor for `stream_idx`
    /// Return true if cursor was advanced
    pub fn advance_cursor(&mut self, stream_idx: usize) -> bool {
        let slot = &mut self.active_rowsets[stream_idx];
        match slot.as_mut() {
            Some(c) => {
                if c.cursor.is_finished() {
                    let sorted = std::mem::take(&mut self.active_rowsets[stream_idx])
                        .expect("exists");
                    self.sorted_rowsets.push(sorted);
                    return false;
                }
                c.cursor.advance();
                true
            }
            None => false,
        }
    }

    /// Returns `true` if the cursor at index `a` is greater than at index `b`
    #[inline]
    pub fn is_gt(&self, stream_a: usize, stream_b: usize) -> bool {
        match (
            &self.active_rowsets[stream_a],
            &self.active_rowsets[stream_b],
        ) {
            (None, _) => true,
            (_, None) => false,
            (Some(ac), Some(bc)) => ac
                .cursor
                .cmp(&bc.cursor)
                .then_with(|| stream_a.cmp(&stream_b))
                .is_gt(),
        }
    }

    /// Returns true if there is an in-progress cursor for a given stream
    pub fn cursor_in_progress(&mut self, stream_idx: usize) -> bool {
        self.active_rowsets[stream_idx]
            .as_ref()
            .map_or(false, |cursor| !cursor.cursor.is_finished())
    }

    /// Yields a sort_order and the sliced [`BatchRowSet`]s
    /// representing (in total) up to N batch size.
    ///
    ///         BatchRowSets
    /// ┌────────────────────────┐
    /// │    Cursor     BatchId  │
    /// │ ┌──────────┐ ┌───────┐ │
    /// │ │  1..7    │ │   A   │ |  (sliced to partial batches)
    /// │ ├──────────┤ ├───────┤ │
    /// │ │  11..14  │ │   B   │ |
    /// │ └──────────┘ └───────┘ │
    /// └────────────────────────┘
    ///
    ///
    ///         SortOrder
    /// ┌─────────────────────────────┐
    /// | (B,11) (A,1) (A,2) (B,12)...|  (up to N rows)
    /// └─────────────────────────────┘
    ///
    /// This will drain the internal state of the builder, and return `None` if there are no pending.
    #[allow(dead_code)]
    pub fn yield_sort_order(&mut self) -> Result<Option<YieldedSortOrder<C>>> {
        if self.is_empty() {
            return Ok(None);
        }

        let sort_order = std::mem::take(&mut self.indices);
        let mut rowsets_to_yield: Vec<BatchRowSet<C>> =
            Vec::with_capacity(self.active_rowsets.capacity());

        // drain already complete sorted_batches
        for mut rowset in std::mem::take(&mut self.sorted_rowsets) {
            rowset.reset();
            rowsets_to_yield.push(rowset);
        }

        // divide: (1) to_yield, (2) to_retain, (3) to_split any in progress cursors
        for stream_idx in 0..self.active_rowsets.len() {
            let mut rowset = match self.active_rowsets[stream_idx].take() {
                Some(c) => c,
                None => continue,
            };

            if rowset.cursor.is_finished() {
                // yield all
                rowset.reset();
                rowsets_to_yield.push(rowset);
            } else if rowset.in_progress() {
                // split

                // current_idx is in the loser tree
                // max pushed row_idx is current_idx -1
                let current_idx = rowset.cursor.current_index();

                let to_yield = rowset.slice(0, current_idx);
                let to_retain = rowset.slice(
                    current_idx,
                    rowset.cursor.cursor_values().len() - current_idx,
                );
                rowsets_to_yield.push(to_yield);
                self.active_rowsets[stream_idx] = Some(to_retain);
            } else {
                // retain all
                self.active_rowsets[stream_idx] = Some(rowset);
            }
        }

        Ok(Some((rowsets_to_yield, sort_order)))
    }
}
