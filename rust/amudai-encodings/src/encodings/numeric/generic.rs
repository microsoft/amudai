use super::{
    AnalysisOutcome, EncodingContext, EncodingKind, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{AsByteSlice, NumericValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, EmptyMetadata, EncodingConfig, EncodingParameters, EncodingPlan,
    Lz4CompressionMode, Lz4Parameters, NullMask, ZstdParameters,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use amudai_io::utils::read_fully;
use std::io::Write;

/// Generic-purpose encoding for numeric values.
pub struct GenericEncoding<T, E> {
    encoder: E,
    _p1: std::marker::PhantomData<T>,
}

impl<T, E> GenericEncoding<T, E> {
    pub fn new(encoder: E) -> Self {
        Self {
            encoder,
            _p1: std::marker::PhantomData,
        }
    }
}

impl<T, E> NumericEncoding<T> for GenericEncoding<T, E>
where
    T: NumericValue,
    E: GenericEncoder,
{
    fn kind(&self) -> EncodingKind {
        self.encoder.encoding_kind()
    }

    fn is_suitable(&self, _config: &EncodingConfig, _stats: &NumericStats<T>) -> bool {
        true
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::empty()
    }

    fn analyze(
        &self,
        values: &[T],
        _null_mask: &NullMask,
        _config: &EncodingConfig,
        stats: &NumericStats<T>,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut encoded_size = 2 + EmptyMetadata::size();
        let mut target = context.buffers.get_buffer();
        self.encoder
            .encode(values.as_byte_slice(), &mut target, None)?;
        encoded_size += target.len();
        Ok(Some(AnalysisOutcome {
            encoding: self.kind(),
            cascading_outcomes: vec![],
            encoded_size,
            compression_ratio: stats.original_size as f64 / encoded_size as f64,
            parameters: Default::default(),
        }))
    }

    fn encode(
        &self,
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let values_bytes = values.as_byte_slice();
        if values_bytes.len() < E::MINIMAL_DATA_SIZE {
            // Fallback to plain encoding if the data is too small.
            return context.numeric_encoders.get::<T>().encode(
                values,
                null_mask,
                target,
                &EncodingPlan {
                    encoding: EncodingKind::Plain,
                    parameters: Default::default(),
                    cascading_encodings: vec![],
                },
                context,
            );
        }

        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        EmptyMetadata::initialize(target).finalize(target);

        self.encoder
            .encode(values_bytes, &mut *target, plan.parameters.as_ref())?;
        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        _value_count: usize,
        _params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        _context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (_, buffer) = EmptyMetadata::read_from(buffer)?;
        self.encoder.decode(buffer, target)
    }

    fn inspect(
        &self,
        _buffer: &[u8],
        _context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![],
        })
    }
}

pub trait GenericEncoder: Sync + Send {
    /// The threshold in terms of data size that is considered
    /// to be minimal for achieving a descent compression ratio with
    /// this encoding.
    const MINIMAL_DATA_SIZE: usize;

    fn encoding_kind(&self) -> EncodingKind;

    /// Encodes bytes slice into `target`.
    fn encode(
        &self,
        bytes: &[u8],
        target: &mut AlignedByteVec,
        parameters: Option<&EncodingParameters>,
    ) -> amudai_common::Result<()>;

    /// Decodes values from `encoded` buffer and appends them to `target`.
    fn decode(&self, encoded: &[u8], target: &mut AlignedByteVec) -> amudai_common::Result<()>;
}

pub struct ZSTDEncoder;

impl GenericEncoder for ZSTDEncoder {
    const MINIMAL_DATA_SIZE: usize = 4096;

    fn encoding_kind(&self) -> EncodingKind {
        EncodingKind::Zstd
    }

    fn encode(
        &self,
        bytes: &[u8],
        target: &mut AlignedByteVec,
        parameters: Option<&EncodingParameters>,
    ) -> amudai_common::Result<()> {
        let level = match parameters {
            Some(EncodingParameters::Zstd(parameters)) => parameters.level,
            _ => ZstdParameters::default().level,
        };
        let mut zstd = zstd::stream::write::Encoder::new(target, level as i32)
            .map_err(|e| Error::io("Failed to create ZSTD encoder", e))?;
        zstd.write_all(bytes)
            .map_err(|e| Error::io("Failed to write ZSTD compressed data", e))?;
        zstd.finish()
            .map_err(|e| Error::io("Failed to finish ZSTD encoder", e))?;
        Ok(())
    }

    fn decode(&self, encoded: &[u8], target: &mut AlignedByteVec) -> amudai_common::Result<()> {
        let mut zstd = zstd::stream::read::Decoder::new(encoded)
            .map_err(|e| Error::io("Failed to create ZSTD decoder", e))?;
        read_all_bytes(&mut zstd, target)
    }
}

pub struct LZ4Encoder;

impl GenericEncoder for LZ4Encoder {
    const MINIMAL_DATA_SIZE: usize = 2048;

    fn encoding_kind(&self) -> EncodingKind {
        EncodingKind::Lz4
    }

    fn encode(
        &self,
        bytes: &[u8],
        target: &mut AlignedByteVec,
        parameters: Option<&EncodingParameters>,
    ) -> amudai_common::Result<()> {
        let compression_mode = match parameters {
            Some(EncodingParameters::Lz4(parameters)) => parameters.mode,
            _ => Lz4Parameters::default().mode,
        };
        let compression_mode = match compression_mode {
            Lz4CompressionMode::Fast => lz4::block::CompressionMode::FAST(1),
            Lz4CompressionMode::HighCompression => lz4::block::CompressionMode::HIGHCOMPRESSION(1),
        };

        // Write uncompressed size that's required for allocating target buffer on decoding.
        target.write_value::<u32>(bytes.len() as u32);

        let max_compressed_size = lz4::block::compress_bound(bytes.len())?;
        let orig_size = target.len();
        target.resize(orig_size + max_compressed_size, 0);
        let enc_buffer = &mut target[orig_size..];
        let size = lz4::block::compress_to_buffer(bytes, Some(compression_mode), false, enc_buffer)
            .map_err(|e| Error::io("Failed to compress block with LZ4", e))?;

        target.truncate(orig_size + size);
        Ok(())
    }

    fn decode(&self, encoded: &[u8], target: &mut AlignedByteVec) -> amudai_common::Result<()> {
        let uncompressed_size = encoded.read_value::<u32>(0) as usize;
        let encoded = &encoded[4..];

        let orig_size = target.len();
        target.resize(orig_size + uncompressed_size, 0);
        let dec_buffer = &mut target[orig_size..];
        let decompressed_size =
            lz4::block::decompress_to_buffer(encoded, Some(uncompressed_size as i32), dec_buffer)
                .map_err(|e| Error::io("Failed to decompress block with LZ4", e))?;

        if decompressed_size != uncompressed_size {
            return Err(Error::invalid_format("LZ4 decompressed size mismatch"));
        }
        Ok(())
    }
}

fn read_all_bytes<R>(read: &mut R, target: &mut AlignedByteVec) -> amudai_common::Result<()>
where
    R: std::io::Read,
{
    let mut buf = [0u8; 1024];
    loop {
        let size = read_fully(&mut *read, &mut buf)?;
        if size == 0 {
            break;
        }
        target.extend_from_slice(&buf[..size]);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::encodings::{
        EncodingConfig, EncodingKind, EncodingPlan, NullMask, numeric::EncodingContext,
    };
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        for encoding in [EncodingKind::Zstd, EncodingKind::Lz4] {
            let context = EncodingContext::new();
            let config = EncodingConfig::default().with_allowed_encodings(&[encoding]);
            let data: Vec<i64> = (0..65536).map(|_| fastrand::i64(-1000..1000)).collect();

            let outcome = context
                .numeric_encoders
                .get::<i64>()
                .analyze(&data, &NullMask::None, &config, &context)
                .unwrap();
            assert!(outcome.is_some());
            let outcome = outcome.unwrap();
            let encoded_size1 = outcome.encoded_size;
            let plan = outcome.into_plan();

            assert_eq!(
                EncodingPlan {
                    encoding,
                    parameters: Default::default(),
                    cascading_encodings: vec![]
                },
                plan,
            );

            let mut encoded = AlignedByteVec::new();
            let encoded_size = context
                .numeric_encoders
                .get::<i64>()
                .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
                .unwrap();
            assert_eq!(encoded_size1, encoded_size);

            // Validate that inspect() returns the same encoding plan as used for encoding
            let inspected_plan = context
                .numeric_encoders
                .get::<i64>()
                .inspect(&encoded, &context)
                .unwrap();
            assert_eq!(plan, inspected_plan);

            let mut decoded = AlignedByteVec::new();
            context
                .numeric_encoders
                .get::<i64>()
                .decode(&encoded, data.len(), None, &mut decoded, &context)
                .unwrap();
            for (a, b) in data.iter().zip(decoded.typed_data::<i64>()) {
                assert_eq!(a, b);
            }
        }
    }
}
