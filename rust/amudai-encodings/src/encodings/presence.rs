use super::{EncodingContext, numeric::value::ValueWriter};
use crate::block_encoder::BlockEncodingProfile;
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use amudai_sequence::presence::{Presence, PresenceBuilder};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PresenceEncodingKind {
    AllNulls = 1,
    Plain = 2,
    RoaringBitmap = 3,
}

impl TryFrom<u8> for PresenceEncodingKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::AllNulls),
            2 => Ok(Self::Plain),
            3 => Ok(Self::RoaringBitmap),
            _ => Err(()),
        }
    }
}

fn encode_presence_with_roaring_bitmap(
    null_mask: &arrow_buffer::NullBuffer,
    mut target: &mut AlignedByteVec,
    is_negated: bool,
) -> amudai_common::Result<usize> {
    let mut roaring = roaring::RoaringBitmap::new();
    for idx in null_mask.valid_indices() {
        roaring.insert(idx as u32);
    }
    let initial_size = target.len();
    target.write_value::<u8>(PresenceEncodingKind::RoaringBitmap as u8);
    target.write_value::<u8>(u8::from(is_negated));
    roaring.serialize_into(&mut target)?;
    Ok(target.len() - initial_size)
}

fn encode_presence_sparse(
    null_mask: &arrow_buffer::NullBuffer,
    target: &mut AlignedByteVec,
    _context: &EncodingContext,
) -> amudai_common::Result<usize> {
    encode_presence_with_roaring_bitmap(null_mask, target, false)
}

fn encode_presence_dense(
    null_mask: &arrow_buffer::NullBuffer,
    target: &mut AlignedByteVec,
    _context: &EncodingContext,
) -> amudai_common::Result<usize> {
    // Convert to sparse presence and encode using RoaringBitmap, which handles
    // sparse presence more efficiently.
    let sparse_null_mask = arrow_buffer::NullBuffer::new(!null_mask.inner());
    encode_presence_with_roaring_bitmap(&sparse_null_mask, target, true)
}

/// Encodes null mask into the target buffer, and returns the
/// number of bytes written.
pub fn encode_presence(
    null_mask: Option<&arrow_buffer::NullBuffer>,
    target: &mut AlignedByteVec,
    context: &EncodingContext,
    profile: BlockEncodingProfile,
) -> amudai_common::Result<usize> {
    let Some(null_mask) = null_mask else {
        return Ok(0);
    };

    if null_mask.null_count() == null_mask.len() {
        target.write_value::<u8>(PresenceEncodingKind::AllNulls as u8);
        return Ok(1);
    }

    if profile != BlockEncodingProfile::Plain {
        let nulls_ratio = null_mask.null_count() as f64 / null_mask.len() as f64;
        let is_dense = nulls_ratio < 0.01;
        if is_dense {
            return encode_presence_dense(null_mask, target, context);
        }
        let is_sparse = nulls_ratio > 0.99;
        if is_sparse {
            return encode_presence_sparse(null_mask, target, context);
        }
    }

    let initial_size = target.len();
    target.write_value::<u8>(PresenceEncodingKind::Plain as u8);
    for value_present in null_mask.iter() {
        target.push_typed::<i8>(if value_present { 1 } else { 0 });
    }
    Ok(target.len() - initial_size)
}

pub fn decode_presence(encoded: &[u8], value_count: usize) -> amudai_common::Result<Presence> {
    if encoded.is_empty() {
        return Ok(Presence::Trivial(value_count));
    }

    let encoding_code = encoded[0];
    let encoding_kind = PresenceEncodingKind::try_from(encoding_code).map_err(|()| {
        Error::invalid_format(format!("Unrecognized presence encoding: {encoding_code}"))
    })?;
    match encoding_kind {
        PresenceEncodingKind::AllNulls => Ok(Presence::Nulls(value_count)),
        PresenceEncodingKind::Plain => {
            let mut presence = AlignedByteVec::new();
            let encoded = &encoded[1..];
            if encoded.len() != value_count {
                return Err(Error::invalid_format(format!(
                    "Invalid presence encoding length: expected {value_count}, got {}",
                    encoded.len()
                )));
            }
            presence.extend_from_slice(encoded);
            Ok(Presence::Bytes(presence))
        }
        PresenceEncodingKind::RoaringBitmap => {
            let is_negated = encoded[1] != 0;
            let roaring = roaring::RoaringBitmap::deserialize_from(&encoded[2..])?;
            let mut builder = PresenceBuilder::new();
            let mut next_pos = 0;
            if is_negated {
                for pos in &roaring {
                    builder.add_n_non_nulls(pos as usize - next_pos);
                    builder.add_null();
                    next_pos = pos as usize + 1;
                }
                builder.add_n_non_nulls(value_count - next_pos);
            } else {
                for pos in &roaring {
                    builder.add_n_nulls(pos as usize - next_pos);
                    builder.add_non_null();
                    next_pos = pos as usize + 1;
                }
                builder.add_n_nulls(value_count - next_pos);
            }
            Ok(builder.build())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block_encoder::BlockEncodingProfile,
        encodings::{
            EncodingContext,
            presence::{
                PresenceEncodingKind, decode_presence, encode_presence,
                encode_presence_with_roaring_bitmap,
            },
        },
    };
    use amudai_bytes::buffer::AlignedByteVec;
    use amudai_sequence::presence::PresenceBuilder;

    #[test]
    fn test_roaring_bitmap_round_trip() {
        const NUM_VALUES: usize = 64000;
        let mut builder = arrow_buffer::NullBufferBuilder::new(NUM_VALUES);
        for _ in 0..NUM_VALUES {
            if fastrand::f64() < 0.3 {
                builder.append_null();
            } else {
                builder.append_non_null();
            }
        }
        let nulls = builder.finish().unwrap();

        for is_negated in [false, true] {
            let null_mask = if is_negated {
                arrow_buffer::NullBuffer::new(!nulls.inner())
            } else {
                nulls.clone()
            };
            let mut encoded = AlignedByteVec::new();
            encode_presence_with_roaring_bitmap(&null_mask, &mut encoded, false).unwrap();
            let encoding_kind = PresenceEncodingKind::try_from(encoded[0]).unwrap();
            assert_eq!(PresenceEncodingKind::RoaringBitmap, encoding_kind);

            let mut expected_presence = PresenceBuilder::new();
            for value in null_mask.inner().iter() {
                if value {
                    expected_presence.add_non_null();
                } else {
                    expected_presence.add_null();
                }
            }
            let expected_presence = expected_presence.build();

            let decoded = decode_presence(&encoded, NUM_VALUES).unwrap();
            assert_eq!(expected_presence, decoded);
        }
    }

    #[test]
    fn test_round_trip() {
        const NUM_VALUES: usize = 64000;
        for (test_desc, nulls_ratio, expected_encoding) in [
            ("all_nulls", 1.0, PresenceEncodingKind::AllNulls),
            ("slightly dense", 0.1, PresenceEncodingKind::Plain),
            ("dense", 0.009, PresenceEncodingKind::RoaringBitmap),
            ("very dense", 0.0009, PresenceEncodingKind::RoaringBitmap),
            ("very sparse", 0.9991, PresenceEncodingKind::RoaringBitmap),
            ("sparse", 0.991, PresenceEncodingKind::RoaringBitmap),
            ("slightly sparse", 0.9, PresenceEncodingKind::Plain),
            ("random", 0.5, PresenceEncodingKind::Plain),
        ] {
            let test_desc = format!("{test_desc} (nulls: {nulls_ratio}%)");
            let mut builder = arrow_buffer::NullBufferBuilder::new(NUM_VALUES);
            let mut mask = Vec::with_capacity(NUM_VALUES);
            mask.extend((0..((NUM_VALUES as f64) * nulls_ratio) as usize).map(|_| false));
            mask.extend((0..NUM_VALUES - mask.len()).map(|_| true));
            fastrand::shuffle(&mut mask);
            let mut expected_presence = PresenceBuilder::new();
            for &is_valid in &mask {
                if is_valid {
                    builder.append_non_null();
                    expected_presence.add_non_null();
                } else {
                    builder.append_null();
                    expected_presence.add_null();
                }
            }
            let nulls = builder.finish().unwrap();
            let expected_presence = expected_presence.build();

            let context = EncodingContext::new();
            let mut encoded = AlignedByteVec::new();
            let encoded_size = encode_presence(
                Some(&nulls),
                &mut encoded,
                &context,
                BlockEncodingProfile::Balanced,
            )
            .unwrap();
            let encoding_kind = PresenceEncodingKind::try_from(encoded[0]).unwrap();
            assert_eq!(
                expected_encoding, encoding_kind,
                "Encoding kinds mismatch ({test_desc})"
            );
            if encoding_kind == PresenceEncodingKind::RoaringBitmap {
                let compression_ratio = nulls.inner().inner().len() as f64 / encoded_size as f64;
                assert!(compression_ratio > 6.0);
            }

            let decoded = decode_presence(&encoded, NUM_VALUES).unwrap();
            assert_eq!(
                expected_presence, decoded,
                "Decoded data mismatch ({test_desc})"
            );
        }
    }
}
