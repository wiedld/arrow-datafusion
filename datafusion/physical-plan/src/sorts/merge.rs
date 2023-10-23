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

//! Merge that deals with an arbitrary size of streaming inputs.
//! This is an order-preserving merge.

use crate::metrics::BaselineMetrics;
use crate::sorts::builder::SortOrderBuilder;
use crate::sorts::cursor::CursorValues;
use crate::RecordBatchStream;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::Stream;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use super::stream::BatchStream;

#[derive(Debug)]
pub(crate) struct SortPreservingMergeStream<C: CursorValues> {
    in_progress: SortOrderBuilder<C>,

    /// The sorted input streams to merge together
    streams: BatchStream<C>,

    /// used to record execution metrics
    metrics: BaselineMetrics,

    /// If the stream has encountered an error
    aborted: bool,

    /// A loser tree that always produces the minimum cursor
    ///
    /// Node 0 stores the top winner, Nodes 1..num_streams store
    /// the loser nodes
    ///
    /// This implements a "Tournament Tree" (aka Loser Tree) to keep
    /// track of the current smallest element at the top. When the top
    /// record is taken, the tree structure is not modified, and only
    /// the path from bottom to top is visited, keeping the number of
    /// comparisons close to the theoretical limit of `log(S)`.
    ///
    /// The current implementation uses a vector to store the tree.
    /// Conceptually, it looks like this (assuming 8 streams):
    ///
    /// ```text
    ///     0 (winner)
    ///
    ///     1
    ///    / \
    ///   2   3
    ///  / \ / \
    /// 4  5 6  7
    /// ```
    ///
    /// Where element at index 0 in the vector is the current winner. Element
    /// at index 1 is the root of the loser tree, element at index 2 is the
    /// left child of the root, and element at index 3 is the right child of
    /// the root and so on.
    ///
    /// reference: <https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree>
    loser_tree: Vec<usize>,

    /// If the most recently yielded overall winner has been replaced
    /// within the loser tree. A value of `false` indicates that the
    /// overall winner has been yielded but the loser tree has not
    /// been updated
    loser_tree_adjusted: bool,

    /// target batch size
    batch_size: usize,

    /// Optional number of rows to fetch
    fetch: Option<usize>,

    /// number of rows produced
    produced: usize,
}

impl<C: CursorValues> SortPreservingMergeStream<C> {
    pub(crate) fn new(
        streams: BatchStream<C>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
    ) -> Self {
        let stream_count = streams.partitions();

        Self {
            in_progress: SortOrderBuilder::new(
                schema,
                stream_count,
                batch_size,
                reservation,
            ),
            streams,
            metrics,
            aborted: false,
            loser_tree: vec![],
            loser_tree_adjusted: false,
            batch_size,
            fetch,
            produced: 0,
        }
    }

    /// If the stream at the given index is not exhausted, and the last cursor for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// cursor for the stream from the returned result
    fn maybe_poll_stream(
        &mut self,
        cx: &mut Context<'_>,
        idx: usize,
    ) -> Poll<Result<()>> {
        if self.in_progress.cursor_in_progress(idx) {
            // Cursor is not finished - don't need to poll next yet
            return Poll::Ready(Ok(()));
        }

        match futures::ready!(self.streams.poll_next(cx, idx)) {
            None => Poll::Ready(Ok(())),
            Some(Err(e)) => Poll::Ready(Err(e)),
            Some(Ok((cursor_values, batch))) => {
                Poll::Ready(self.in_progress.push_batch(idx, cursor_values, batch))
            }
        }
    }

    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }
        // try to initialize the loser tree
        if self.loser_tree.is_empty() {
            // Ensure all non-exhausted streams have a cursor from which
            // rows can be pulled
            for i in 0..self.streams.partitions() {
                if let Err(e) = ready!(self.maybe_poll_stream(cx, i)) {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
            self.init_loser_tree();
        }

        // NB timer records time taken on drop, so there are no
        // calls to `timer.done()` below.
        let elapsed_compute = self.metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        loop {
            // Adjust the loser tree if necessary, returning control if needed
            if !self.loser_tree_adjusted {
                let winner = self.loser_tree[0];
                if let Err(e) = ready!(self.maybe_poll_stream(cx, winner)) {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
                self.update_loser_tree();
            }

            let stream_idx = self.loser_tree[0];
            if self.in_progress.advance_cursor(stream_idx) {
                self.loser_tree_adjusted = false;
                self.in_progress.push_row(stream_idx);

                // stop sorting if fetch has been reached
                if self.fetch_reached() {
                    self.aborted = true;
                } else if self.in_progress.len() < self.batch_size {
                    continue;
                }
            }

            self.produced += self.in_progress.len();

            return Poll::Ready(self.in_progress.build_record_batch().transpose());
        }
    }

    fn fetch_reached(&mut self) -> bool {
        self.fetch
            .map(|fetch| self.produced + self.in_progress.len() >= fetch)
            .unwrap_or(false)
    }

    /// Find the leaf node index in the loser tree for the given cursor index
    ///
    /// Note that this is not necessarily a leaf node in the tree, but it can
    /// also be a half-node (a node with only one child). This happens when the
    /// number of cursors/streams is not a power of two. Thus, the loser tree
    /// will be unbalanced, but it will still work correctly.
    ///
    /// For example, with 5 streams, the loser tree will look like this:
    ///
    /// ```text
    ///           0 (winner)
    ///
    ///           1
    ///        /     \
    ///       2       3
    ///     /  \     / \
    ///    4    |   |   |
    ///   / \   |   |   |
    /// -+---+--+---+---+---- Below is not a part of loser tree
    ///  S3 S4 S0   S1  S2
    /// ```
    ///
    /// S0, S1, ... S4 are the streams (read: stream at index 0, stream at
    /// index 1, etc.)
    ///
    /// Zooming in at node 2 in the loser tree as an example, we can see that
    /// it takes as input the next item at (S0) and the loser of (S3, S4).
    ///
    #[inline]
    fn lt_leaf_node_index(&self, cursor_index: usize) -> usize {
        (self.streams.partitions() + cursor_index) / 2
    }

    /// Find the parent node index for the given node index
    #[inline]
    fn lt_parent_node_index(&self, node_idx: usize) -> usize {
        node_idx / 2
    }

    /// Attempts to initialize the loser tree with one value from each
    /// non exhausted input, if possible
    fn init_loser_tree(&mut self) {
        // Init loser tree
        self.loser_tree = vec![usize::MAX; self.streams.partitions()];
        for i in 0..self.streams.partitions() {
            let mut winner = i;
            let mut cmp_node = self.lt_leaf_node_index(i);
            while cmp_node != 0 && self.loser_tree[cmp_node] != usize::MAX {
                let challenger = self.loser_tree[cmp_node];
                if self.in_progress.is_gt(winner, challenger) {
                    self.loser_tree[cmp_node] = winner;
                    winner = challenger;
                }

                cmp_node = self.lt_parent_node_index(cmp_node);
            }
            self.loser_tree[cmp_node] = winner;
        }
        self.loser_tree_adjusted = true;
    }

    /// Attempts to update the loser tree, following winner replacement, if possible
    fn update_loser_tree(&mut self) {
        let mut winner = self.loser_tree[0];
        // Replace overall winner by walking tree of losers
        let mut cmp_node = self.lt_leaf_node_index(winner);
        while cmp_node != 0 {
            let challenger = self.loser_tree[cmp_node];
            if self.in_progress.is_gt(winner, challenger) {
                self.loser_tree[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node = self.lt_parent_node_index(cmp_node);
        }
        self.loser_tree[0] = winner;
        self.loser_tree_adjusted = true;
    }
}

impl<C: CursorValues + Unpin> Stream for SortPreservingMergeStream<C> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
    }
}

impl<C: CursorValues + Unpin> RecordBatchStream for SortPreservingMergeStream<C> {
    fn schema(&self) -> SchemaRef {
        self.in_progress.schema().clone()
    }
}
