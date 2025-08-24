use crate::defs::shard::FieldDescriptor;
use amudai_bloom_filters::config::XXH3_64_ALGORITHM;
use amudai_bytes::Bytes;

impl FieldDescriptor {
    /// Attempts to get the SBBF (Split Block Bloom Filter) bytes and hash seed from the field descriptor.
    /// Returns Some((aligned_bytes, hash_seed)) if the field has an SBBF with data and a supported hash algorithm, None otherwise.
    pub fn try_get_sbbf_data(&self) -> Option<(Bytes, u64)> {
        if let Some(membership_filters) = &self.membership_filters {
            if let Some(sbbf_proto) = &membership_filters.sbbf {
                // Validate hash algorithm - only XXH3_64 is supported
                if sbbf_proto.hash_algorithm != XXH3_64_ALGORITHM {
                    return None;
                }

                // Create aligned bytes from the protobuf data
                let aligned_bytes = Bytes::copy_from_slice(&sbbf_proto.data);
                assert!(aligned_bytes.is_aligned(32));
                return Some((aligned_bytes, sbbf_proto.hash_seed));
            }
        }
        None
    }
}
