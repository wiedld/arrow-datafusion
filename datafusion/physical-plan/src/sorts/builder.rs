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

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use std::mem::take;

use super::batches::{BatchCursor, BatchId};
use super::cursor::CursorValues;

pub type YieldedSortOrder<C> = (Vec<SortOrder>, Vec<BatchCursor<C>>);
pub type SortOrder = (BatchId, usize); // (batch_id, row_idx)

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
#[derive(Debug)]
pub struct SortOrderBuilder<C: CursorValues> {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Maintain a list of cursors for each finished (sorted) batch.
    /// These batch_cursors will be fully yielded.
    /// 
    /// Note: the number of total batches can be larger than the number of total streams.
    sorted_batches: Vec<BatchCursor<C>>,

    /// The current [`BatchCursor`] for each stream.
    /// These batch_cursors are in_progress (being sorted), and will be partially yielded.
    cursors: Vec<Option<BatchCursor<C>>>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_id, row_idx)`
    indices: Vec<SortOrder>,
}

impl<C: CursorValues> SortOrderBuilder<C> {
    /// Create a new [`SortOrderBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(
        schema: SchemaRef,
        stream_count: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            schema,
            sorted_batches: Vec::with_capacity(stream_count * 2),
            cursors: (0..stream_count).map(|_| None).collect(),
            indices: Vec::with_capacity(batch_size),
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(
        &mut self,
        stream_idx: usize,
        batch_cursor: BatchCursor<C>,
    ) -> Result<()> {
        self.cursors[stream_idx] = Some(batch_cursor);
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        let batch_cursor = self.cursors[stream_idx]
            .as_mut()
            .expect("row pushed for non-existant cursor");
        self.indices.push((batch_cursor.batch_id(), batch_cursor.row_idx));
        batch_cursor.row_idx += 1;
    }

    /// Returns true if there is an in-progress cursor for a given stream
    pub fn cursor_in_progress(&mut self, stream_idx: usize) -> bool {
        self.cursors[stream_idx]
            .as_mut()
            .map_or(false, |batch_cursor| !batch_cursor.cursor.is_finished())
    }

    /// Advance the cursor for `stream_idx`
    /// Return true if cursor was advanced
    pub fn advance_cursor(&mut self, stream_idx: usize) -> bool {
        let slot = &mut self.cursors[stream_idx];
        match slot.as_mut() {
            Some(c) => {
                if c.cursor.is_finished() {
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
    pub fn is_gt(&mut self, stream_idx_a: usize, stream_idx_b: usize) -> bool {
        match (
            self.cursor_in_progress(stream_idx_a),
            self.cursor_in_progress(stream_idx_b),
        ) {
            (false, _) => true,
            (_, false) => false,
            _ => match (&self.cursors[stream_idx_a], &self.cursors[stream_idx_b]) {
                (Some(a), Some(b)) => a
                    .cursor
                    .cmp(&b.cursor)
                    .then_with(|| stream_idx_a.cmp(&stream_idx_b))
                    .is_gt(),
                _ => unreachable!(),
            },
        }
    }

    /// Returns the number of in-progress rows in this [`SortOrderBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`SortOrderBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the schema of this [`SortOrderBuilder`]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Yields a sort_order and the sliced [`BatchCursor`]s
    /// representing (in total) up to N batch size.
    ///
    ///         BatchCursors
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

        let sort_order = take(&mut self.indices);
        let mut cursors_to_yield: Vec<BatchCursor<C>> =
            Vec::with_capacity(self.cursors.capacity());

        // drain already complete sorted_batches
        for mut batch_cursor in take(&mut self.sorted_batches) {
            batch_cursor.reset();
            cursors_to_yield.push(batch_cursor);
        }

        // split any in_progress cursor
        for stream_idx in 0..self.cursors.len() {
            let mut batch_cursor = match self.cursors[stream_idx].take() {
                Some(c) => c,
                None => continue,
            };

            if batch_cursor.cursor.is_finished() {
                batch_cursor.reset();
                cursors_to_yield.push(batch_cursor);
            } else if batch_cursor.in_progress() {
                unimplemented!(
                    "have not yet merged the cursor slicing PR"
                );
            } else {
                // retained all (nothing yielded)
                self.cursors[stream_idx] = Some(batch_cursor);
            }
        }

        Ok(Some((cursors_to_yield, sort_order)))
    }
}
