pub mod index_format;
pub mod index_table;
pub mod lookup_header;
pub mod uniform_lookup;

use bytes::{BufMut, BytesMut};
use index_table::IndexTable;
use minibytes::Bytes;
use std::collections::BTreeMap;

use crate::{key_shape::KeySpaceDesc, wal::WalPosition};

use std::sync::LazyLock;
// use single_hop::UniformLookupIndex;
// pub static PERSISTED_INDEX: LazyLock<UniformLookupIndex> =
//     LazyLock::new(|| UniformLookupIndex::new());

use lookup_header::LookupHeaderIndex;
pub static INDEX_FORMAT: LazyLock<LookupHeaderIndex> =
    LazyLock::new(|| LookupHeaderIndex::new_with_default_metrics());

/// Writes key-value pairs from IndexTable to a BytesMut buffer
/// Returns the populated buffer
pub fn serialize_index_entries(
    table: &IndexTable,
    ks: &KeySpaceDesc,
    initial_capacity: usize,
    header_size: usize,
) -> BytesMut {
    let mut out = BytesMut::with_capacity(initial_capacity);

    // If header is needed, reserve space for it
    if header_size > 0 {
        out.put_bytes(0, header_size);
    }

    // Write each key-value pair
    for (key, value) in table.data.iter() {
        if key.len() != ks.reduced_key_size() {
            panic!(
                "Index in ks {} contains key length {} (configured {})",
                ks.name(),
                key.len(),
                ks.reduced_key_size()
            );
        }
        out.put_slice(&key);
        value.write_to_buf(&mut out);
    }

    out
}

/// Deserializes IndexTable from bytes
/// - data_offset: Where actual data begins (after any headers)
/// - ks: KeySpaceDesc to determine element sizes
/// - b: Source bytes
/// Returns the deserialized IndexTable
pub fn deserialize_index_entries(data_offset: usize, ks: &KeySpaceDesc, b: Bytes) -> IndexTable {
    let data = if data_offset > 0 {
        b.slice(data_offset..)
    } else {
        b
    };

    let element_size = ks.reduced_key_size() + WalPosition::LENGTH;
    let elements = data.len() / element_size;
    assert_eq!(
        data.len(),
        elements * element_size,
        "Index data size is not a multiple of element size"
    );

    let mut table_data = BTreeMap::new();
    for i in 0..elements {
        let key = data.slice(i * element_size..(i * element_size + ks.reduced_key_size()));
        let value = WalPosition::from_slice(
            &data[(i * element_size + ks.reduced_key_size())..(i * element_size + element_size)],
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
