use crate::cell::CellId;
use minibytes::Bytes;

pub mod db_iterator;

pub(crate) struct IteratorResult<T> {
    pub cell: Option<CellId>,
    pub key: Bytes,
    pub value: T,
}

impl<T> IteratorResult<T> {
    /// Replaces key and value in the result with provided values and returns new iterator result
    pub fn with_key_value<V>(self, key: Bytes, value: V) -> IteratorResult<V> {
        IteratorResult {
            cell: self.cell,
            key,
            value,
        }
    }
}
