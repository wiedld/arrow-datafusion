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

use crate::metrics::BaselineMetrics;
use crate::sorts::cursor::CursorValues;
use crate::RecordBatchStream;
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::Stream;
use std::collections::HashMap;
use std::marker::Send;
use std::task::{ready, Context, Poll};
use std::{pin::Pin, sync::Arc};

use super::batches::{BatchId, BatchTracker};
use super::builder::YieldedSortOrder;
use super::merge::SortPreservingMergeStream;
use super::stream::{BatchStream, BatchTrackerStream, MergeStream};

/// Sort preserving cascade stream
///
/// The cascade works as a tree of sort-preserving-merges, where each merge has
/// a limited fan-in (number of inputs) and a limit size yielded (batch size) per poll.
/// The poll is called from the root merge, which will poll its children, and so on.
///
/// ```text
/// ┌─────┐                ┌─────┐                                                           
/// │  2  │                │  1  │                                                           
/// │  3  │                │  2  │                                                           
/// │  1  │─ ─▶  sort  ─ ─▶│  2  │─ ─ ─ ─ ─ ─ ─ ─ ┐                                          
/// │  4  │                │  3  │                                                           
/// │  2  │                │  4  │                │                                          
/// └─────┘                └─────┘                                                           
/// ┌─────┐                ┌─────┐                ▼                                          
/// │  1  │                │  1  │                                                           
/// │  4  │─ ▶  sort  ─ ─ ▶│  1  ├ ─ ─ ─ ─ ─ ▶ merge  ─ ─ ─ ─                                
/// │  1  │                │  4  │                           │                               
/// └─────┘                └─────┘                                                           
///   ...                   ...                ...           ▼                               
///                                                                                          
///                                                       merge  ─ ─ ─ ─ ─ ─ ▶ sorted output
///                                                                               stream     
///                                                          ▲                               
///   ...                   ...                ...           │                               
/// ┌─────┐                ┌─────┐                                                           
/// │  3  │                │  3  │                           │                               
/// │  1  │─ ▶  sort  ─ ─ ▶│  1  │─ ─ ─ ─ ─ ─▶ merge  ─ ─ ─ ─                                
/// └─────┘                └─────┘                                                           
/// ┌─────┐                ┌─────┐                ▲                                          
/// │  4  │                │  3  │                                                           
/// │  3  │─ ▶  sort  ─ ─ ▶│  4  │─ ─ ─ ─ ─ ─ ─ ─ ┘                                          
/// └─────┘                └─────┘                                                           
///                                                                                          
/// in_mem_batches                   do a series of merges that                              
///                                  each has a limited fan-in                               
///                                  (number of inputs)                                      
/// ```
///
///
/// The cascade is built using a series of streams, each with a different purpose:
///   * Streams leading into the leaf nodes:
///      1. [`BatchStream`] yields the initial cursors and batches. (e.g. a RowCursorStream)
///         * This initial stream is for a number of partitions (e.g. 100).
///         * only a single BatchStream.
///      2. TODO:
///         * split the single stream, across multiple leaf nodes.
///      3. [`BatchTrackerStream`] is used to collect the record batches from the leaf nodes.
///         * contains a single, shared use of [`BatchTracker`].
///         * polling of streams is non-blocking across streams/partitions.
///      4. BatchTrackerStream yields a [`BatchCursorStream`](super::streams::BatchCursorStream)
///
/// * Streams between merge nodes:
///      1. a single [`MergeStream`] is yielded per node.
///      2. TODO:
///         * adapter to interleave sort_order
///         * converts a [`MergeStream`] to a [`BatchCursorStream`](super::streams::BatchCursorStream)
///
pub(crate) struct SortPreservingCascadeStream<C: CursorValues> {
    /// If the stream has encountered an error, or fetch is reached
    aborted: bool,

    /// The sorted input streams to merge together
    /// TODO: this will become the root of the cascade tree
    cascade: MergeStream<C>,

    /// Batches are collected on first yield from the [`BatchStream`].
    /// Subsequent merges in cascade all refer to the [`BatchId`] using [`BatchCursor`](super::batches::BatchCursor)s.
    record_batch_collector: Arc<BatchTracker>,

    /// used to record execution metrics
    metrics: BaselineMetrics,

    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,
}

impl<C: CursorValues + Send + Unpin + 'static> SortPreservingCascadeStream<C> {
    pub(crate) fn new(
        streams: BatchStream<C>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
    ) -> Self {
        let batch_tracker = Arc::new(BatchTracker::new(reservation.new_empty()));

        let streams = BatchTrackerStream::new(streams, batch_tracker.clone());

        Self {
            aborted: false,
            metrics: metrics.clone(),
            record_batch_collector: batch_tracker,
            schema: schema.clone(),
            cascade: Box::pin(SortPreservingMergeStream::new(
                Box::new(streams),
                schema,
                metrics,
                batch_size,
                fetch,
                reservation,
            )),
        }
    }

    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        match ready!(self.cascade.as_mut().poll_next(cx)) {
            None => Poll::Ready(None),
            Some(Err(e)) => {
                self.aborted = true;
                Poll::Ready(Some(Err(e)))
            }
            Some(Ok(yielded_sort_order)) => {
                match self.build_record_batch(yielded_sort_order) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(e) => {
                        self.aborted = true;
                        Poll::Ready(Some(Err(e)))
                    }
                }
            }
        }
    }

    /// Construct and yield the root node [`RecordBatch`]s.
    fn build_record_batch(
        &mut self,
        yielded_sort_order: YieldedSortOrder<C>,
    ) -> Result<RecordBatch> {
        let (batch_cursors, sort_order) = yielded_sort_order;

        let mut batches_needed = Vec::with_capacity(sort_order.len());
        let mut batches_seen: HashMap<BatchId, (usize, usize)> =
            HashMap::with_capacity(sort_order.len()); // (batch_idx, max_row_idx)

        let row_idx_offsets = batch_cursors.iter().fold(
            HashMap::with_capacity(batch_cursors.len()),
            |mut acc, batch_cursor| {
                acc.insert(
                    batch_cursor.batch_id(),
                    batch_cursor.get_offset_from_abs_idx(),
                );
                acc
            },
        );
        let mut adjusted_sort_order = Vec::with_capacity(sort_order.len());

        for (batch_id, row_idx) in sort_order.iter() {
            let batch_idx = match batches_seen.get(batch_id) {
                Some((batch_idx, _)) => *batch_idx,
                None => {
                    let batch_idx = batches_seen.len();
                    batches_needed.push(*batch_id);
                    batch_idx
                }
            };
            adjusted_sort_order.push((batch_idx, row_idx_offsets[batch_id] + *row_idx));
            batches_seen
                .insert(*batch_id, (batch_idx, row_idx_offsets[batch_id] + *row_idx));
        }

        let batches = self
            .record_batch_collector
            .get_batches(batches_needed.as_slice());

        // remove record_batches (from the batch tracker) that are fully yielded
        let batches_to_remove = batches
            .iter()
            .zip(batches_needed)
            .filter_map(|(batch, batch_id)| {
                let max_row_idx = batches_seen[&batch_id].1;
                if batch.num_rows() == max_row_idx + 1 {
                    Some(batch_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // record_batch data to yield
        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = batches
                    .iter()
                    .map(|batch| batch.column(column_idx).as_ref())
                    .collect();
                Ok(interleave(&arrays, adjusted_sort_order.as_slice())?)
            })
            .collect::<Result<Vec<_>>>()?;

        self.record_batch_collector
            .remove_batches(batches_to_remove.as_slice());

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl<C: CursorValues + Send + Unpin + 'static> Stream for SortPreservingCascadeStream<C> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.metrics.record_poll(poll)
    }
}

impl<C: CursorValues + Send + Unpin + 'static> RecordBatchStream
    for SortPreservingCascadeStream<C>
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
