use bytes::Buf;
use bytes::BufMut;

pub const MAX_U16_VARINT: u16 = 0x7FFF;

const MAX_SINGLE_BYTE_EXCLUDED: u16 = 0x80;

/// Serializes a `u16` using a custom variable-length encoding into the provided buffer.
/// Panics if the value exceeds the encodable limit (32767).
pub fn serialize_u16_varint(value: u16, buf: &mut impl BufMut) {
    if value > MAX_U16_VARINT {
        panic!("Value {value} exceeds maximum encodable value {MAX_U16_VARINT}");
    }

    if value < MAX_SINGLE_BYTE_EXCLUDED {
        buf.put_u8(value as u8);
    } else {
        let low = (value & 0x7F) as u8;
        let high = (value >> 7) as u8;
        buf.put_u8((MAX_SINGLE_BYTE_EXCLUDED as u8) | low);
        buf.put_u8(high);
    }
}

/// Deserializes a `u16` from a custom variable-length format using `Buf`.
/// Panics if input is invalid or incomplete.
pub fn deserialize_u16_varint(buf: &mut impl Buf) -> u16 {
    if !buf.has_remaining() {
        panic!("Attempted to deserialize from empty buffer");
    }

    let first = buf.get_u8();

    if first < 0x80 {
        first as u16
    } else {
        if buf.remaining() < 1 {
            panic!("Insufficient bytes for two-byte encoding");
        }

        let second = buf.get_u8();
        let low = (first & 0x7F) as u16;
        let high = (second as u16) << 7;
        high | low
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u16_var_int_serialization() {
        use bytes::{Bytes, BytesMut};

        let values = [
            0u16,
            MAX_SINGLE_BYTE_EXCLUDED - 1,
            MAX_SINGLE_BYTE_EXCLUDED + 1,
            MAX_SINGLE_BYTE_EXCLUDED,
            300,
            255,
            32767,
        ];

        for &val in &values {
            let mut buf = BytesMut::new();
            serialize_u16_varint(val, &mut buf);
            if val < MAX_SINGLE_BYTE_EXCLUDED {
                assert_eq!(buf.len(), 1);
            } else {
                assert_eq!(buf.len(), 2);
            }

            let mut read_buf = Bytes::from(buf);
            let decoded = deserialize_u16_varint(&mut read_buf);

            assert_eq!(val, decoded);
        }
    }
}
