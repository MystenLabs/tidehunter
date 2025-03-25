use crate::cell::CellId;
use minibytes::Bytes;

pub mod db_iterator;

pub struct IteratorResult<T> {
    pub next_cell: Option<CellId>,
    pub next_key: Option<Bytes>,
    pub key: Bytes,
    pub value: T,
}

impl<T> IteratorResult<T> {
    /// Replaces key and value in the result with provided values and returns new iterator result
    pub fn with_key_value<V>(self, key: Bytes, value: V) -> IteratorResult<V> {
        IteratorResult {
            next_cell: self.next_cell,
            next_key: self.next_key,
            key,
            value,
        }
    }
}
