pub mod index_format;
pub mod index_table;
pub mod lookup_header;
pub mod uniform_lookup;
pub mod utils;

use bytes::{BufMut, BytesMut};
use index_table::IndexTable;
use minibytes::Bytes;
use std::collections::BTreeMap;

use crate::{key_shape::KeySpaceDesc, wal::WalPosition};

pub fn index_element_size(ks: &KeySpaceDesc) -> usize {
    ks.index_key_size() + WalPosition::LENGTH
}

/// Writes key-value pairs from IndexTable to a BytesMut buffer
/// Returns the populated buffer
pub fn serialize_index_entries(table: &IndexTable, ks: &KeySpaceDesc, out: &mut BytesMut) {
    // Write each key-value pair
    for (key, value) in table.data.iter() {
        if key.len() != ks.index_key_size() {
            panic!(
                "Index in ks {} contains key length {} (configured {})",
                ks.name(),
                key.len(),
                ks.index_key_size()
            );
        }
        out.put_slice(&key);
        value.write_to_buf(out);
    }
}

/// Deserializes IndexTable from bytes
/// - data_offset: Where actual data begins (after any headers)
/// - ks: KeySpaceDesc to determine element sizes
/// - b: Source bytes
/// Returns the deserialized IndexTable
pub fn deserialize_index_entries(ks: &KeySpaceDesc, data: Bytes) -> IndexTable {
    let element_size = ks.index_key_size() + WalPosition::LENGTH;
    let elements = data.len() / element_size;
    assert_eq!(
        data.len(),
        elements * element_size,
        "Index data size is not a multiple of element size"
    );

    let mut table_data = BTreeMap::new();
    for i in 0..elements {
        let key = data.slice(i * element_size..(i * element_size + ks.index_key_size()));
        let value = WalPosition::from_slice(
            &data[(i * element_size + ks.index_key_size())..(i * element_size + element_size)],
        );
        table_data.insert(key, value);
    }

    assert_eq!(
        table_data.len(),
        elements,
        "Duplicate keys detected in index"
    );
    IndexTable { data: table_data }
}
