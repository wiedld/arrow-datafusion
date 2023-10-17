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

use std::cmp::Ordering;
use std::sync::Arc;

use super::rowset::{FieldsSet, RowSet, RowsSet};
use super::values::FieldValues;

/// A [`Cursor`] for a given [`RowSet`]
pub struct Cursor<R> {
    cur_row: usize,
    rows: Arc<R>,
}

impl<R> std::fmt::Debug for Cursor<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SortKeyCursor").finish()
    }
}

impl<R: RowSet> Cursor<R> {
    /// Create a new SortKeyCursor from [`RowSet`].
    pub fn new(rows: Arc<R>) -> Self {
        Self { cur_row: 0, rows }
    }

    /// Returns the current row
    fn current(&self) -> Option<R::Item<'_>> {
        self.rows.fetch(self.cur_row)
    }

    /// Returns true when cursor is advanced beyond bounds
    pub fn is_finished(&self) -> bool {
        self.rows.size() == self.cur_row
    }

    /// Advances cursor.
    /// Does not check bounds.
    pub fn advance(&mut self) -> usize {
        let t = self.cur_row;
        self.cur_row += 1;
        t
    }

    #[allow(dead_code)] // TODO: access inner RowSet, then RowSet::slice()
    /// Provide refcount access to inner
    pub fn inner(&self) -> Arc<R> {
        self.rows.clone()
    }
}

impl PartialEq for Cursor<RowsSet> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<T: FieldValues> PartialEq for Cursor<FieldsSet<T>>
where
    <T as FieldValues>::Value: 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Cursor<RowsSet> {}

impl<T: FieldValues> Eq for Cursor<FieldsSet<T>> where <T as FieldValues>::Value: 'static {}

impl PartialOrd for Cursor<RowsSet> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: FieldValues> PartialOrd for Cursor<FieldsSet<T>>
where
    <T as FieldValues>::Value: 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cursor<RowsSet> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.current(), other.current()) {
            (Some(a), Some(b)) => a.cmp(&b),
            _ => panic!("arrow::Row is not nullable"),
        }
    }
}

impl<T: FieldValues> Ord for Cursor<FieldsSet<T>>
where
    <T as FieldValues>::Value: 'static,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let options = self.rows.options();

        match (self.current(), other.current()) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => match options.nulls_first {
                true => Ordering::Less,
                false => Ordering::Greater,
            },
            (Some(_), None) => match options.nulls_first {
                true => Ordering::Greater,
                false => Ordering::Less,
            },
            (Some(s_v), Some(o_v)) => match options.descending {
                true => T::compare(o_v, s_v),
                false => T::compare(s_v, o_v),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::buffer::ScalarBuffer;
    use arrow_array::types::Int32Type;
    use arrow_array::PrimitiveArray;
    use arrow_buffer::{BooleanBuffer, NullBuffer};
    use arrow_schema::SortOptions;

    use crate::sorts::rowset::FieldsSet;
    use crate::sorts::values::PrimitiveValues;

    use super::*;

    fn new_primitive(
        options: SortOptions,
        values: ScalarBuffer<i32>,
        null_count: usize,
    ) -> Cursor<FieldsSet<PrimitiveValues<i32>>> {
        let buf = match options.nulls_first {
            true => [
                vec![false; null_count],
                vec![true; values.len() - null_count],
            ]
            .concat(),
            false => [
                vec![true; values.len() - null_count],
                vec![false; null_count],
            ]
            .concat(),
        };

        let array = PrimitiveArray::<Int32Type>::new(
            values,
            Some(NullBuffer::new(BooleanBuffer::from(buf))),
        );
        Cursor::new(Arc::new(FieldsSet::new(options, &array)))
    }

    #[test]
    fn test_primitive_nulls_first() {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MAX, 1, 2, 3]);
        let mut a = new_primitive(options, buffer, 1);
        let buffer = ScalarBuffer::from(vec![1, 2, -2, -1, 1, 9]);
        let mut b = new_primitive(options, buffer, 2);

        // NULL == NULL
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL == NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL < -2
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 1 > -2
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 1 > -1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 1 == 1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // 9 > 1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 9 > 2
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let buffer = ScalarBuffer::from(vec![0, 1, i32::MIN, i32::MAX]);
        let mut a = new_primitive(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![-1, i32::MAX, i32::MIN]);
        let mut b = new_primitive(options, buffer, 2);

        // 0 > -1
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 0 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 1 < NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // NULL = NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        let options = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let buffer = ScalarBuffer::from(vec![6, 1, i32::MIN, i32::MAX]);
        let mut a = new_primitive(options, buffer, 3);
        let buffer = ScalarBuffer::from(vec![67, -3, i32::MAX, i32::MIN]);
        let mut b = new_primitive(options, buffer, 2);

        // 6 > 67
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 6 < -3
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // NULL == NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        let options = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MIN, i32::MAX, 6, 3]);
        let mut a = new_primitive(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![i32::MAX, 4546, -3]);
        let mut b = new_primitive(options, buffer, 1);

        // NULL == NULL
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL == NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL < 4546
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 > 4546
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 6 < -3
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);
    }
}
