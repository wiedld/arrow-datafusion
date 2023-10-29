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
use crate::stream::ReceiverStreamBuilder;
use crate::RecordBatchStream;
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::{Stream, StreamExt};
use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::marker::Send;
use std::task::{ready, Context, Poll};
use std::{pin::Pin, sync::Arc};

use super::batches::{BatchId, BatchTracker};
use super::builder::YieldedSortOrder;
use super::merge::SortPreservingMergeStream;
use super::stream::{
    BatchStream, BatchTrackerStream, InterleaveMergeStream, MergeStream,
};

static MAX_STREAMS_PER_MERGE: usize = 10;

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
///      2. `BatchStream::take_partitions` allows leave nodes to `mem::take` a subset of partitions.
///         * This splits the single stream, across multiple leaf nodes.
///      3. [`BatchTrackerStream`] is used to collect the record batches from the leaf nodes.
///         * contains a single, shared use of [`BatchTracker`].
///         * polling of streams is non-blocking across streams/partitions.
///      4. BatchTrackerStream yields a [`BatchRowSetStream`](super::stream::BatchRowSetStream)
///
/// * Streams between merge nodes:
///      1. a single [`MergeStream`] is yielded per node.
///      2. [`InterleaveMergeStream`] is used in non-root nodes.
///         * converts a [`MergeStream`] to a [`BatchRowSetStream`](super::stream::BatchRowSetStream)
///         * the MergeStream has [`BatchRowSet`](super::batches::BatchRowSet)s sliced to what was yielded with a sort_order.
///             * sort_order = ((batch_0, row_0), (batch_0, row_1), (batch_1, row_0), (batch_0, row_2), ...)
///             * sliced batch_0 from rows 0-2.
///         * BatchRowSetStream provide the next merge node the ordered [`BatchRowSet`](super::batches::BatchRowSet)s
///             * poll yields ((batch_0, row_0), (batch_0, row_1))
///             * poll yields ((batch_1, row_0))
///             * poll yields ((batch_0, row_2), ...)
///      3. next merge node consumes [`BatchRowSetStream`](super::stream::BatchRowSetStream)
///  
/// * For the last merge node;
///      1. skip the additional interleaving ([`InterleaveMergeStream`])
///      2. perform the `arrow::compute::interleave` and yield the record batch
///             
///
///
///
/// Together, these streams make for a composable tree of merge nodes:
/// ```text
///
/// ┌─────────────────────────────────┐
/// │         BatchStream             │
/// │ ┌────────────┐ ┌──────────────┐ │
/// │ │CursorValues│ │ RecordBatches│ │
/// │ └────────────┘ └──────────────┘ │
/// └─────────────────────────────────┘
///                 │
///          BatchTrackerStream
///                 │
///                 ▼
/// ┌────────────────────────────────────────┐
/// │           BatchRowSetStream            │ <─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │ ┌────────┐ ┌─────────┐ ┌─────────────┐ │                            |
/// │ │ Cursor │ │ BatchId │ │ BatchOffset │ │                            |
/// │ └────────┘ └─────────┘ └─────────────┘ │                            |
/// └────────────────────────────────────────┘                            |
///                 │                                                     |
///    SortPreservingMergeStream (a.k.a. merge node)                      |
///                 │                                                     |
///                 ▼                                            InterleaveMergeStream
/// ┌───────────────────────────────────────────────┐                     |
/// │           MergeStream                         │                     |
/// │ ┌─────────────────────────────┐ ┌───────────┐ │                     |
/// │ │        BatchRowSet          │ │ SortOrder │ │                     |
/// │ │ (Cursor/BatchId/BatchOffset)│ │           │ │                     |
/// │ └─────────────────────────────┘ └───────────┘ │                     |
/// └───────────────────────────────────────────────┘                     |
///             /               \                                         |
///        Root Node       Non-root node                                  |
///            |                |                                         |
///            |                └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///            |
///            ▼
///   `arrow::compute::interleave`
///            |
///            ▼
///       RecordBatch
///
///  ```
pub(crate) struct SortPreservingCascadeStream<C: CursorValues> {
    /// If the stream has encountered an error, or fetch is reached
    aborted: bool,

    /// The sorted input streams to merge together
    /// TODO: this will become the root of the cascade tree
    cascade: MergeStream<C>,

    /// Batches are collected on first yield from the [`BatchStream`].
    /// Subsequent merges in cascade all refer to the [`BatchId`] using [`BatchRowSet`](super::batches::BatchRowSet)s.
    record_batch_collector: Arc<BatchTracker>,

    /// used to record execution metrics
    metrics: BaselineMetrics,

    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,
}

impl<C: CursorValues + Send + Unpin + 'static> SortPreservingCascadeStream<C> {
    pub(crate) fn new<S: BatchStream<C, Output = Result<(C, RecordBatch)>>>(
        mut streams: S,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
    ) -> Self {
        let stream_count = streams.partitions();
        let batch_tracker = Arc::new(BatchTracker::new(reservation.new_empty()));

        let max_streams_per_merge = MAX_STREAMS_PER_MERGE;
        let mut divided_streams: VecDeque<MergeStream<C>> =
            VecDeque::with_capacity(stream_count / max_streams_per_merge + 1);

        // build leaves
        for stream_idx in (0..stream_count).step_by(max_streams_per_merge) {
            let limit = std::cmp::min(max_streams_per_merge, stream_count - stream_idx);

            // divide the BatchCursorStream across multiple leafnode merges.
            let streams = BatchTrackerStream::new(
                streams.take_partitions(0..limit),
                batch_tracker.clone(),
            );

            divided_streams.push_back(spawn_buffered_merge(
                Box::pin(SortPreservingMergeStream::new(
                    Box::new(streams),
                    metrics.clone(),
                    batch_size,
                    None, // fetch, the LIMIT, is applied to the final merge
                )),
                2,
            ));
        }

        // build rest of tree
        let mut next_level: VecDeque<MergeStream<C>> =
            VecDeque::with_capacity(divided_streams.len() / max_streams_per_merge + 1);
        while divided_streams.len() > 1 || !next_level.is_empty() {
            let fan_in: Vec<MergeStream<C>> = divided_streams
                .drain(0..std::cmp::min(max_streams_per_merge, divided_streams.len()))
                .collect();

            next_level.push_back(spawn_buffered_merge(
                Box::pin(SortPreservingMergeStream::new(
                    Box::new(InterleaveMergeStream::new(fan_in)),
                    metrics.clone(),
                    batch_size,
                    if divided_streams.is_empty() && next_level.is_empty() {
                        fetch
                    } else {
                        None
                    }, // fetch, the LIMIT, is applied to the final merge
                )),
                2,
            ));
            // in order to maintain sort-preserving streams, don't mix the merge tree levels.
            if divided_streams.is_empty() {
                divided_streams = std::mem::take(&mut next_level);
            }
        }

        Self {
            aborted: false,
            metrics: metrics.clone(),
            record_batch_collector: batch_tracker,
            schema: schema.clone(),
            cascade: divided_streams
                .remove(0)
                .expect("must have a root merge stream"),
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
    ///
    /// ```text
    ///
    ///         SortOrder
    ///  (row_idx relative to Cursor)
    /// ┌───────────────────────────┐
    /// | (B,11) (F,101) (A,1) ...  ┼ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
    /// └───────────────────────────┘                      |
    ///                                                    |
    ///                                                    |
    ///         BatchRowSets                               |
    /// ┌──────────────────────────────────────┐           |
    /// │    Cursor     BatchId   BatchOffset  │           |
    /// │ ┌──────────┐ ┌───────┐ ┌───────────┐ │           |
    /// │ │  1..7    │ │   A   │ │     5     │ ┼ ─ ─ ─ ─ ─ |
    /// │ ├──────────┤ ├───────┤ │───────────│ │           |
    /// │ │  11..14  │ │   B   │ │     2     │ ┼ ─ ─ ─ ─ ─ |
    /// │ └──────────┘ └───────┘ └───────────┘ │           |
    /// │     ...         ...         ...      │           |
    /// │ ┌──────────┐ ┌───────┐ ┌───────────┐ │           |
    /// │ │ 101..103 │ │   F   │ │    15     │ ┼ ─ ─ ─ ─ ─ |
    /// │ ├──────────┤ ├───────┤ │───────────│ │           |
    /// │ │ 151..157 │ │   G   │ │     2     │ ┼ ─ ─ ─ ─ ─ |
    /// │ └──────────┘ └───────┘ └───────────┘ │           |
    /// └──────────────────────────────────────┘           |
    ///                                                    |
    ///             ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┘
    ///             |
    ///             ▼
    ///         SortOrder
    ///      (absolute row_idx)
    /// ┌───────────────────────────┐
    /// | (B,13) (F,116) (A,6) ...  ┼ ─ ─ ─ ─ ─ ─ ─ ─ ─
    /// └───────────────────────────┘                  |
    ///                                                |
    ///        BatchTracker                            |
    /// ┌────────────────────────┐                     |
    /// │    Batch      BatchId  │           ┌─────────▼────────────┐
    /// │ ┌──────────┐ ┌───────┐ │    ─ ─ ─ ▶│     interleave       |
    /// │ │  <data>  │ │   A   │ ┼ ─ │       └─────────|────────────┘
    /// │ ├──────────┤ ├───────┤ │   │                 |
    /// │ │  <data>  │ │   B   │ ┼ ─ │                 |
    /// │ └──────────┘ └───────┘ │   │                 ▼
    /// │     ...         ...    │   │          batch up to N rows
    /// │ ┌──────────┐ ┌───────┐ │   │
    /// │ │  <data>  │ │   F   │ ┼ ─ │
    /// │ ├──────────┤ ├───────┤ │   │
    /// │ │  <data>  │ │   G   │ ┼ ─ ┘
    /// │ └──────────┘ └───────┘ │
    /// └────────────────────────┘
    ///
    /// ```
    fn build_record_batch(
        &mut self,
        yielded_sort_order: YieldedSortOrder<C>,
    ) -> Result<RecordBatch> {
        let (batch_rowsets, sort_order) = yielded_sort_order;

        let mut batches_to_interleave = Vec::with_capacity(sort_order.len());
        let mut batches_seen: HashMap<BatchId, (usize, usize)> =
            HashMap::with_capacity(sort_order.len()); // (batch_idx, max_row_idx)

        let row_idx_offsets = batch_rowsets.iter().fold(
            HashMap::with_capacity(batch_rowsets.len()),
            |mut acc, rowset| {
                acc.insert(rowset.batch_id(), rowset.get_offset_from_abs_idx());
                acc
            },
        );
        let mut adjusted_sort_order = Vec::with_capacity(sort_order.len());

        for (batch_id, row_idx) in sort_order.iter() {
            let batch_idx = match batches_seen.entry(*batch_id) {
                Entry::Occupied(entry) => entry.get().0,
                Entry::Vacant(entry) => {
                    let batch_idx = batches_to_interleave.len();
                    batches_to_interleave.push(*batch_id);
                    entry.insert((batch_idx, *row_idx));
                    batch_idx
                }
            };
            adjusted_sort_order.push((batch_idx, row_idx_offsets[batch_id] + *row_idx));
        }

        let batches = self
            .record_batch_collector
            .get_batches(batches_to_interleave.as_slice());

        // remove record_batches (from the batch tracker) that are fully yielded
        let batches_to_remove = batches
            .iter()
            .zip(batches_to_interleave)
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

fn spawn_buffered_merge<C: CursorValues + Send + 'static>(
    mut input: MergeStream<C>,
    capacity: usize,
) -> MergeStream<C> {
    // Use tokio only if running from a multi-thread tokio context
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
        {
            let mut builder = ReceiverStreamBuilder::new(capacity);

            let sender = builder.tx();

            builder.spawn(async move {
                while let Some(item) = input.next().await {
                    if sender.send(item).await.is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            });

            builder.build()
        }
        _ => input,
    }
}
