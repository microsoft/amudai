use super::{
    offsets::{analyze_offsets, decode_offsets_and_make_sequence, encode_offsets},
    stats::{BinaryStats, BinaryStatsCollectorFlags},
    BinaryValuesSequence, StringEncoding,
};
use crate::encodings::{
    numeric::value::{ValueReader, ValueWriter},
    AlignedEncMetadata, AnalysisOutcome, EncodingConfig, EncodingContext, EncodingKind,
    EncodingParameters, EncodingPlan, NullMask, ALIGNMENT_BYTES,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{presence::Presence, sequence::ValueSequence};
use fsst;

/// Fast Static Symbol Table (FSST) compression encoding.
/// https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf
pub struct FSSTEncoding;

impl FSSTEncoding {
    pub fn new() -> Self {
        Self
    }
}

impl StringEncoding for FSSTEncoding {
    fn kind(&self) -> EncodingKind {
        EncodingKind::FSST
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &BinaryStats) -> bool {
        const FSST_SYMTAB_MAX_SIZE: u32 = 8 * 255 + 255;
        if stats.original_size < 4 * FSST_SYMTAB_MAX_SIZE {
            // It's not worth encoding with FSST if the original size is not big enough.
            return false;
        }
        true
    }

    fn stats_collector_flags(&self) -> BinaryStatsCollectorFlags {
        BinaryStatsCollectorFlags::empty()
    }

    fn analyze(
        &self,
        values: &BinaryValuesSequence,
        _null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &BinaryStats,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut encoded_size = 2 + FSSTMetadata::size();

        let values_data = values.values();
        let compressor = fsst::Compressor::train(&vec![values_data]);

        // TODO: Check how to reuse compresesed values vector here.
        let compressed_values = compressor.compress(values_data);

        encoded_size += compressor.symbol_table().len() * std::mem::size_of::<u64>();
        encoded_size += compressor.symbol_lengths().len();
        encoded_size += compressed_values.len();
        encoded_size = encoded_size.next_multiple_of(ALIGNMENT_BYTES);

        let (offsets_outcome, offsets_size) = analyze_offsets(values, config, context)?;
        encoded_size += offsets_size;

        Ok(Some(AnalysisOutcome {
            encoding: self.kind(),
            cascading_outcomes: vec![offsets_outcome],
            encoded_size,
            compression_ratio: stats.original_size as f64 / encoded_size as f64,
            parameters: Default::default(),
        }))
    }

    fn encode(
        &self,
        values: &BinaryValuesSequence,
        _null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = FSSTMetadata::initialize(target);
        let values_data = values.values();
        let compressor = fsst::Compressor::train(&vec![values_data]);

        // Write the FSST symbol table.
        let symbols: &[u64] = unsafe { std::mem::transmute(compressor.symbol_table()) };
        target.write_values(symbols);
        target.write_values(compressor.symbol_lengths());
        metadata.symbols_count = symbols.len();

        // Write FSST encoded values.
        // TODO: Check how to reuse compresesed values vector here.
        let compressed_values = compressor.compress(values_data);
        target.write_values(&compressed_values);
        metadata.values_size = compressed_values.len();

        let alignment = target.len().next_multiple_of(ALIGNMENT_BYTES);
        target.resize(alignment, 0);

        let (offset_width, offsets_cascading, _) = encode_offsets(
            values,
            target,
            plan.cascading_encodings[0].as_ref(),
            context,
        )?;

        metadata.offset_width = offset_width;
        metadata.offsets_cascading = offsets_cascading;
        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        presence: Presence,
        type_desc: BasicTypeDescriptor,
        _params: &EncodingParameters,
        context: &EncodingContext,
    ) -> amudai_common::Result<ValueSequence> {
        let (metadata, buffer) = FSSTMetadata::read_from(buffer)?;

        let symbols_size = metadata.symbols_count * std::mem::size_of::<u64>();
        if buffer.len() < symbols_size + metadata.symbols_count + metadata.values_size {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }

        // Read FSST symbols.
        let mut symbols = context.buffers.get_buffer();
        symbols.extend_from_slice(&buffer[..symbols_size]);
        let symbols: &[fsst::Symbol] = unsafe { std::mem::transmute(symbols.typed_data::<u64>()) };
        let offset = symbols_size;

        // Read FSST symbol lengths.
        let symbol_lengths = &buffer[offset..offset + metadata.symbols_count];
        let decompressor = fsst::Decompressor::new(symbols, &symbol_lengths);
        let offset = offset + metadata.symbols_count;

        // Decode values.
        let compressed = &buffer[offset..offset + metadata.values_size];
        let mut values = AlignedByteVec::new();
        // Size as required by the FSST API.
        values.resize_zeroed::<u8>(decompressor.max_decompression_capacity(compressed));
        let decoded = unsafe { std::mem::transmute(values.as_mut_slice()) };
        let size = decompressor.decompress_into(compressed, decoded);
        values.truncate(size);

        let offset = offset + metadata.values_size;
        let offset = offset.next_multiple_of(ALIGNMENT_BYTES);

        decode_offsets_and_make_sequence(
            &buffer[offset..],
            values,
            presence,
            type_desc,
            metadata.offset_width,
            metadata.offsets_cascading,
            context,
        )
    }
}

struct FSSTMetadata {
    start_offset: usize,
    /// Number of symbols in the FSST symbol table (0-255).
    pub symbols_count: usize,
    /// Encoded values size.
    pub values_size: usize,
    /// Whether the offsets encoding is cascading.
    pub offsets_cascading: bool,
    /// Values offsets width.
    pub offset_width: u8,
}

impl AlignedEncMetadata for FSSTMetadata {
    fn own_size() -> usize {
        7
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            symbols_count: 0,
            values_size: 0,
            offsets_cascading: false,
            offset_width: 0,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u8>(self.start_offset, self.symbols_count as u8);
        target.write_value_at::<u32>(self.start_offset + 1, self.values_size as u32);
        target.write_value_at::<u8>(
            self.start_offset + 5,
            if self.offsets_cascading { 1 } else { 0 },
        );
        target.write_value_at::<u8>(self.start_offset + 6, self.offset_width as u8);
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                symbols_count: buffer.read_value::<u8>(0) as usize,
                values_size: buffer.read_value::<u32>(1) as usize,
                offsets_cascading: buffer.read_value::<u8>(5) != 0,
                offset_width: buffer.read_value::<u8>(6),
            },
            &buffer[Self::size()..],
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{borrow::Cow, vec};

    #[test]
    pub fn test_round_trip() {
        let context = EncodingContext::new();
        let config = EncodingConfig::default().with_disallowed_encodings(&[EncodingKind::ZSTD]);

        let unique_values = [
            b"aaaaaaaa" as &[u8],
            b"bbbbbbbb",
            b"cccccccc",
            b"dddddddd",
            b"eeeeeeee",
        ];
        let values = (0..10)
            .map(|_| {
                (0..1000)
                    .map(|_| fastrand::choice(unique_values.iter()).unwrap().to_vec())
                    .reduce(|a, b| [a, vec![b' '], b].concat())
                    .unwrap()
                    .to_vec()
            })
            .collect::<Vec<_>>();

        let array = arrow_array::array::LargeBinaryArray::from_iter_values(values.iter());
        let sequence = BinaryValuesSequence::ArrowLargeBinaryArray(Cow::Owned(array));
        let outcome = context
            .binary_encoders
            .analyze(&sequence, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let parameters = outcome.parameters.clone();
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::FSST,
                parameters: parameters.clone(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::Delta,
                    parameters: Default::default(),
                    cascading_encodings: vec![Some(EncodingPlan {
                        encoding: EncodingKind::SingleValue,
                        parameters: Default::default(),
                        cascading_encodings: vec![]
                    })]
                })]
            },
            plan
        );

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = context
            .binary_encoders
            .encode(&sequence, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        let decoded = context
            .binary_encoders
            .decode(
                &encoded,
                Presence::Trivial(values.len()),
                Default::default(),
                &Default::default(),
                &context,
            )
            .unwrap();

        assert_eq!(values.len(), decoded.presence.len());
        for (a, b) in values.iter().zip(decoded.binary_values().unwrap()) {
            assert_eq!(std::str::from_utf8(a.as_slice()), std::str::from_utf8(b));
        }
    }
}
