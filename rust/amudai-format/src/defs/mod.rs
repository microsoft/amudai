#[path = "./amudai.common.rs"]
pub mod common;

pub mod anyvalue_ext;
pub mod bytes_list_ext;
pub mod data_ref_ext;
pub mod field_ext;
pub mod hash_lookup_ext;
pub mod shard_ext;
pub mod stripe_ext;

#[path = "./amudai.shard.rs"]
pub mod shard;

#[allow(clippy::needless_lifetimes)]
#[path = "./schema.fbs.rs"]
pub mod schema;

pub mod schema_ext;

pub const AMUDAI_MAGIC: [u8; 4] = [0x61, 0x6d, 0x75, 0x64];
pub const AMUDAI_VERSION_MAJOR: u8 = 0;
pub const AMUDAI_VERSION_MINOR: u16 = 1;
pub const AMUDAI_VERSION_PATCH: u8 = 0;
pub const AMUDAI_HEADER: [u8; 8] = [
    0x61,
    0x6d,
    0x75,
    0x64,
    AMUDAI_VERSION_PATCH,
    (AMUDAI_VERSION_MINOR & 0xff) as u8,
    (AMUDAI_VERSION_MINOR >> 8) as u8,
    AMUDAI_VERSION_MAJOR,
];
pub const AMUDAI_FOOTER: [u8; 8] = AMUDAI_HEADER;

/// Minimum possible size of the Amudai shard directory blob:
/// * header:`u64`
/// * directory_len:`u32`
/// * ... // directory
/// * directory_checksum:`u32`
/// * directory_len:`u32`
/// * footer:`u64`
pub const SHARD_DIRECTORY_FILE_MIN_SIZE: usize =
    AMUDAI_HEADER.len() * 2 + std::mem::size_of::<u32>() * 3;

/// File format header size.
pub const AMUDAI_HEADER_SIZE: usize = AMUDAI_HEADER.len();

/// File format footer size.
pub const AMUDAI_FOOTER_SIZE: usize = AMUDAI_FOOTER.len();

/// Size of the serialized protobuf message length prefix.
pub const MESSAGE_LEN_SIZE: usize = 4;

/// Size of the message checksum suffix.
pub const CHECKSUM_SIZE: usize = 4;

/// Alignment of the encoded buffer start position within the storage artifact.
pub const ENCODED_BUFFER_ARTIFACT_ALIGNMENT: usize = 64;

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{
        common::{AnyValue, NameValuePair, any_value},
        shard::ShardProperties,
    };

    #[test]
    fn test_basic_proto_serialization() {
        let mut props = ShardProperties::default();
        props.standard_properties.push(NameValuePair {
            name: "name".into(),
            value: Some(AnyValue {
                annotation: None,
                kind: Some(any_value::Kind::I64Value(1)),
            }),
        });

        let buf = props.encode_to_vec();
        assert!(buf.len() < 20);

        let props = ShardProperties::decode(buf.as_slice()).unwrap();
        assert_eq!(props.standard_properties[0].name, "name");
        assert_eq!(
            props.standard_properties[0]
                .value
                .as_ref()
                .unwrap()
                .as_i64()
                .unwrap(),
            1
        );
    }
}
