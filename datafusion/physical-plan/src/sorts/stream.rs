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

use crate::sorts::batches::{BatchCursor, BatchTracker};
use crate::sorts::builder::YieldedSortOrder;
use crate::sorts::cursor::{ArrayValues, CursorArray, CursorValues, RowValues};
use crate::SendableRecordBatchStream;
use crate::{PhysicalExpr, PhysicalSortExpr};
use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::stream::{Fuse, StreamExt};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// A fallible [`PartitionedStream`] of [`Cursor`] and [`RecordBatch`]
pub type BatchStream<C> = Box<dyn PartitionedStream<Output = Result<(C, RecordBatch)>>>;

/// A [`PartitionedStream`] of [`BatchCursor`]s
pub type BatchCursorStream<C> =
    Box<dyn PartitionedStream<Output = Result<BatchCursor<C>>>>;

/// A stream of yielded [`SortOrder`](super::builder::SortOrder)s, with the corresponding [`BatchCursor`]s, is a [`MergeStream`].
///
/// Each merge node (a.k.a. `SortPreservingMergeStream` + `SortOrderBuilder`), will yield a [`MergeStream`].
pub(crate) type MergeStream<C> =
    Pin<Box<dyn futures::Stream<Item = Result<YieldedSortOrder<C>>> + Send>>;

/// A [`Stream`](futures::Stream) that has multiple partitions that can
/// be polled separately but not concurrently
///
/// Used by sort preserving merge to decouple the cursor merging logic from
/// the source of the cursors, the intention being to allow preserving
/// any row encoding performed for intermediate sorts
pub trait PartitionedStream: std::fmt::Debug + Send {
    type Output;

    /// Returns the number of partitions
    fn partitions(&self) -> usize;

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>>;
}

/// A newtype wrapper around a set of fused [`SendableRecordBatchStream`]
/// that implements debug, and skips over empty [`RecordBatch`]
struct FusedStreams(Vec<Fuse<SendableRecordBatchStream>>);

impl std::fmt::Debug for FusedStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedStreams")
            .field("num_streams", &self.0.len())
            .finish()
    }
}

impl FusedStreams {
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match ready!(self.0[stream_idx].poll_next_unpin(cx)) {
                Some(Ok(b)) if b.num_rows() == 0 => continue,
                r => return Poll::Ready(r),
            }
        }
    }
}

/// A [`PartitionedStream`] that wraps a set of [`SendableRecordBatchStream`]
/// and computes [`RowValues`] based on the provided [`PhysicalSortExpr`]
#[derive(Debug)]
pub struct RowCursorStream {
    /// Converter to convert output of physical expressions
    converter: RowConverter,
    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,
    /// Input streams
    streams: FusedStreams,
    /// Tracks the memory used by `converter`
    reservation: MemoryReservation,
}

impl RowCursorStream {
    pub fn try_new(
        schema: &Schema,
        expressions: &[PhysicalSortExpr],
        streams: Vec<SendableRecordBatchStream>,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        let sort_fields = expressions
            .iter()
            .map(|expr| {
                let data_type = expr.expr.data_type(schema)?;
                Ok(SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()?;

        let streams = streams.into_iter().map(|s| s.fuse()).collect();
        let converter = RowConverter::new(sort_fields)?;
        Ok(Self {
            converter,
            reservation,
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            streams: FusedStreams(streams),
        })
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<RowValues> {
        let cols = self
            .column_expressions
            .iter()
            .map(|expr| Ok(expr.evaluate(batch)?.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;

        let rows = self.converter.convert_columns(&cols)?;
        self.reservation.try_resize(self.converter.size())?;

        // track the memory in the newly created Rows.
        let mut rows_reservation = self.reservation.new_empty();
        rows_reservation.try_grow(rows.size())?;
        Ok(RowValues::new(rows, rows_reservation))
    }
}

impl PartitionedStream for RowCursorStream {
    type Output = Result<(RowValues, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch)?;
                Ok((cursor, batch))
            })
        }))
    }
}

/// Specialized stream for sorts on single primitive columns
pub struct FieldCursorStream<T: CursorArray> {
    /// The physical expressions to sort by
    sort: PhysicalSortExpr,
    /// Input streams
    streams: FusedStreams,
    phantom: PhantomData<fn(T) -> T>,
}

impl<T: CursorArray> std::fmt::Debug for FieldCursorStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveCursorStream")
            .field("num_streams", &self.streams)
            .finish()
    }
}

impl<T: CursorArray> FieldCursorStream<T> {
    pub fn new(sort: PhysicalSortExpr, streams: Vec<SendableRecordBatchStream>) -> Self {
        let streams = streams.into_iter().map(|s| s.fuse()).collect();
        Self {
            sort,
            streams: FusedStreams(streams),
            phantom: Default::default(),
        }
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<ArrayValues<T::Values>> {
        let value = self.sort.expr.evaluate(batch)?;
        let array = value.into_array(batch.num_rows());
        let array = array.as_any().downcast_ref::<T>().expect("field values");
        Ok(ArrayValues::new(self.sort.options, array))
    }
}

impl<T: CursorArray> PartitionedStream for FieldCursorStream<T> {
    type Output = Result<(ArrayValues<T::Values>, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch)?;
                Ok((cursor, batch))
            })
        }))
    }
}

/// Converts a [`BatchStream`] to a [`BatchCursorStream`].
///
/// Collects the [`RecordBatch`] per poll,
/// and only passes along the [`BatchCursor`].
pub(crate) struct BatchTrackerStream<C: CursorValues> {
    // Partitioned Input stream.
    streams: BatchStream<C>,
    record_batch_holder: Arc<BatchTracker>,
}

impl<C: CursorValues> BatchTrackerStream<C> {
    pub fn new(streams: BatchStream<C>, record_batch_holder: Arc<BatchTracker>) -> Self {
        Self {
            streams,
            record_batch_holder,
        }
    }
}

impl<C: CursorValues> PartitionedStream for BatchTrackerStream<C> {
    type Output = Result<BatchCursor<C>>;

    fn partitions(&self) -> usize {
        self.streams.partitions()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|(cursor_values, batch)| {
                let batch_id = self.record_batch_holder.add_batch(batch)?;
                Ok(BatchCursor::new(batch_id, cursor_values))
            })
        }))
    }
}

impl<C: CursorValues> std::fmt::Debug for BatchTrackerStream<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchTrackerStream").finish()
    }
}
