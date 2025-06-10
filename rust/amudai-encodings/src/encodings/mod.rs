use crate::buffers_pool::BuffersPool;
use amudai_bytes::buffer::AlignedByteVec;
use binary::BinaryEncodings;
use numeric::NumericEncodingsPool;
use std::{borrow::Cow, collections::HashSet};
use tinyvec::ArrayVec;

pub mod binary;
pub mod numeric;
pub mod presence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum EncodingKind {
    Plain = 1,
    FLBitPack = 2,
    FrameOfReference = 3,
    FusedFrameOfReference = 4,
    Delta = 5,
    RunLength = 6,
    SingleValue = 7,
    TruncateU8 = 8,
    TruncateU16 = 9,
    TruncateU32 = 10,
    TruncateU64 = 11,
    Alp = 12,
    AlpRd = 13,
    Dictionary = 14,
    SharedDictionary = 15,
    Fsst = 16,
    Zstd = 17,
    Lz4 = 18,
    ZigZag = 19,
    Sparse = 20,
}

impl TryFrom<u16> for EncodingKind {
    type Error = ();

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(EncodingKind::Plain),
            2 => Ok(EncodingKind::FLBitPack),
            3 => Ok(EncodingKind::FrameOfReference),
            4 => Ok(EncodingKind::FusedFrameOfReference),
            5 => Ok(EncodingKind::Delta),
            6 => Ok(EncodingKind::RunLength),
            7 => Ok(EncodingKind::SingleValue),
            8 => Ok(EncodingKind::TruncateU8),
            9 => Ok(EncodingKind::TruncateU16),
            10 => Ok(EncodingKind::TruncateU32),
            11 => Ok(EncodingKind::TruncateU64),
            12 => Ok(EncodingKind::Alp),
            13 => Ok(EncodingKind::AlpRd),
            14 => Ok(EncodingKind::Dictionary),
            15 => Ok(EncodingKind::SharedDictionary),
            16 => Ok(EncodingKind::Fsst),
            17 => Ok(EncodingKind::Zstd),
            18 => Ok(EncodingKind::Lz4),
            19 => Ok(EncodingKind::ZigZag),
            20 => Ok(EncodingKind::Sparse),
            _ => Err(()),
        }
    }
}

/// Result of analysing a sequence of values to determine the best encoding scheme.
#[derive(Debug)]
pub struct AnalysisOutcome {
    /// Encoding kind that was applied.
    pub encoding: EncodingKind,

    /// Outcomes of encodings that were applied on nested streams
    /// if any. For instance, top encoding RLE produces two nested streams:
    /// values and lengths. Either of them can be encoded using a cascading
    /// encoding or not, depending on efficiency. In this case, this vector
    /// will contain two elements: one for values and one for lengths.
    pub cascading_outcomes: Vec<Option<AnalysisOutcome>>,

    /// Encoded data size in bytes.
    pub encoded_size: usize,

    /// Calculated compression ratio of the encoded sequence of values.
    pub compression_ratio: f64,

    /// Encoding specific parameters that were used to encode the data.
    pub parameters: EncodingParameters,
}

impl AnalysisOutcome {
    pub fn into_plan(self) -> EncodingPlan {
        EncodingPlan {
            encoding: self.encoding,
            parameters: self.parameters,
            cascading_encodings: self
                .cascading_outcomes
                .into_iter()
                .map(|outcome| outcome.map(AnalysisOutcome::into_plan))
                .collect(),
        }
    }
}

/// Defines constraints to be used when selecting the best encoding scheme.
#[derive(Debug, Clone)]
pub struct EncodingConfig {
    /// Minimum compression ratio required for the encoder to be selected.
    pub min_compression_ratio: f64,

    /// Maximum number of cascading levels, including the top encoding.
    pub max_cascading_levels: u8,

    /// Encodings that are only allowed to be used during analysis phase.
    pub allowed_encodings: Option<HashSet<EncodingKind>>,

    /// Encodings that are prohibited from being used during analysis phase.
    pub disallowed_encodings: Option<HashSet<EncodingKind>>,

    /// Whether cascading encodings may use all the available encodings.
    pub cascading_use_all_encodings: bool,
}

impl EncodingConfig {
    const DEFAULT_CASCADING_LEVEL_ENCODINGS: &'static [EncodingKind] = &[
        EncodingKind::Delta,
        EncodingKind::FrameOfReference,
        EncodingKind::FusedFrameOfReference,
        EncodingKind::FLBitPack,
        EncodingKind::RunLength,
        EncodingKind::SingleValue,
        EncodingKind::TruncateU8,
        EncodingKind::TruncateU16,
        EncodingKind::TruncateU32,
        EncodingKind::TruncateU64,
    ];

    /// Create a configuration for a next cascading level encoding.
    ///
    /// If `cascading_use_all_encodings` is set to `false` (default), then
    /// only a subset of encodings is allowed to be used for cascading levels.
    pub fn make_cascading_config(&self) -> Self {
        let mut config = EncodingConfig::default()
            .with_max_cascading_levels(self.max_cascading_levels.saturating_sub(1))
            // Lower encoding levels must not have the same compression ratio requirement.
            // What's important that they are contributing to the overall compression ratio.
            .with_min_compression_ratio(1.25);

        if !self.cascading_use_all_encodings {
            config = config.with_allowed_encodings(Self::DEFAULT_CASCADING_LEVEL_ENCODINGS);
        }
        config
    }

    pub fn with_max_cascading_levels(&self, levels: u8) -> Self {
        let mut config = self.clone();
        config.max_cascading_levels = levels;
        config
    }

    pub fn with_min_compression_ratio(&self, ratio: f64) -> Self {
        let mut config = self.clone();
        config.min_compression_ratio = ratio;
        config
    }

    pub fn with_allowed_encodings(&self, encodings: &[EncodingKind]) -> Self {
        let mut config = self.clone();
        config.allowed_encodings = Some(encodings.iter().cloned().collect());
        config
    }

    pub fn with_disallowed_encodings(&self, encodings: &[EncodingKind]) -> Self {
        let mut config = self.clone();
        config.disallowed_encodings = Some(encodings.iter().cloned().collect());
        config
    }

    pub fn is_encoding_allowed(&self, encoding: EncodingKind) -> bool {
        if !self
            .allowed_encodings
            .as_ref()
            .is_none_or(|encodings| encodings.contains(&encoding))
        {
            return false;
        }
        if self
            .disallowed_encodings
            .as_ref()
            .is_some_and(|encodings| encodings.contains(&encoding))
        {
            return false;
        }
        true
    }

    pub fn with_cascading_use_all_encodings(&self) -> Self {
        let mut config = self.clone();
        config.cascading_use_all_encodings = true;
        config
    }
}

impl Default for EncodingConfig {
    fn default() -> Self {
        Self {
            min_compression_ratio: 2.0,
            max_cascading_levels: 4,
            allowed_encodings: None,
            disallowed_encodings: None,
            cascading_use_all_encodings: false,
        }
    }
}

/// Stack of encodings to apply on a stream of values including
/// the specification of what cascading to use for every
/// nested stream of values.
///
/// The plan disables collecting statistics and choosing the best
/// encoding scheme depending on compression efficiency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingPlan {
    /// Encoding kind to apply on the top level stream of values.
    pub encoding: EncodingKind,

    /// Encoding specific parameters to use.
    pub parameters: EncodingParameters,

    /// Encodings to apply on nested streams of values.
    pub cascading_encodings: Vec<Option<EncodingPlan>>,
}

/// Encoding specific parameters to be reused when encoding sequence of values.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub enum EncodingParameters {
    #[default]
    None,
    Alp(AlpParameters),
    AlpRd(AlpRdParameters),
    Dictionary(DictionaryParameters),
    Zstd(ZstdParameters),
    Lz4(Lz4Parameters),
}

/// ALP encoding parameters to be reused when encoding block of values
/// that have the same origin and, potentially, have the same distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlpParameters {
    pub exponent_e: u8,
    pub factor_f: u8,
}

/// Reusable parameters for the ALP-RD encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlpRdParameters {
    /// The bit width of the right part of the floating-point value.
    pub right_bit_width: u8,
    /// The dictionary of the left parts.
    pub left_parts_dict: ArrayVec<[u16; numeric::alprd::MAX_DICT_SIZE]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryParameters {
    /// Shared binary values dictionary containing values sorted by popularity.
    /// The dictionary is a list of strings with their popularity count.
    /// Value index plus one is a dictionary code (zero is reserved for null value).
    pub shared_dict: Vec<(Vec<u8>, u32)>,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ZstdCompressionLevel {
    #[default]
    Default = 0,
    MinimalFastest = 1,
    MaximalSlowest = 22,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZstdParameters {
    pub level: ZstdCompressionLevel,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lz4CompressionMode {
    #[default]
    Fast,
    HighCompression,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lz4Parameters {
    pub mode: Lz4CompressionMode,
}

pub struct EncodingContext {
    /// Pool of numeric encoder instances.
    pub numeric_encoders: NumericEncodingsPool,

    /// Binary values encoders set.
    pub binary_encoders: BinaryEncodings,

    /// Pool of buffers to reduce memory allocations during encoding/decoding.
    pub buffers: BuffersPool,
}

impl EncodingContext {
    pub fn new() -> Self {
        Self {
            numeric_encoders: NumericEncodingsPool::new(),
            binary_encoders: BinaryEncodings::new(),
            buffers: BuffersPool::new(),
        }
    }
}

impl Default for EncodingContext {
    fn default() -> Self {
        Self::new()
    }
}

pub enum NullMask<'a> {
    /// A null mask that indicates that no values are null.
    None,
    /// A null mask based on Arrow's null bitmap.
    Arrow(Cow<'a, arrow_buffer::NullBuffer>),
}

impl NullMask<'_> {
    /// Returns the number of null values in the mask.
    #[inline]
    fn null_count(&self) -> usize {
        match self {
            NullMask::None => 0,
            NullMask::Arrow(null_buffer) => null_buffer.null_count(),
        }
    }

    /// Returns `true` if the value at the given index is null.
    #[inline]
    fn is_null(&self, index: usize) -> bool {
        match self {
            NullMask::None => false,
            NullMask::Arrow(null_buffer) => null_buffer.is_null(index),
        }
    }
}

impl<'a> From<Option<&'a arrow_buffer::NullBuffer>> for NullMask<'a> {
    fn from(buffer: Option<&'a arrow_buffer::NullBuffer>) -> Self {
        match buffer {
            Some(buffer) => NullMask::Arrow(Cow::Borrowed(buffer)),
            None => NullMask::None,
        }
    }
}

impl From<Option<arrow_buffer::NullBuffer>> for NullMask<'_> {
    fn from(buffer: Option<arrow_buffer::NullBuffer>) -> Self {
        match buffer {
            Some(buffer) => NullMask::Arrow(Cow::Owned(buffer)),
            None => NullMask::None,
        }
    }
}

impl From<NullMask<'_>> for Option<arrow_buffer::NullBuffer> {
    fn from(val: NullMask<'_>) -> Self {
        match val {
            NullMask::None => None,
            NullMask::Arrow(buffer) => Some(buffer.into_owned()),
        }
    }
}

/// Encoded sections must be aligned as required by some encodings (e.g. BitPacking).
/// Therefore, it must be guaranteed that encoded sections are always aligned to
/// this constant. It's a responsibility of the encoding procedure to ensure that.
pub const ALIGNMENT_BYTES: usize = 8;

/// The trait for encoding metadata that ensures alignment of the metadata section.
pub trait AlignedEncMetadata {
    /// Returns the size of the metadata section.
    fn own_size() -> usize;

    /// Returns the size of the metadata section including the alignment padding.
    fn size() -> usize {
        // Reduce 2 bytes to account for the encoding kind written before
        // the metatada section.
        (Self::own_size() + 2).next_multiple_of(ALIGNMENT_BYTES) - 2
    }

    /// Preserves the space for the metadata section in the target buffer.
    fn initialize(target: &mut AlignedByteVec) -> Self;

    /// Writes the metadata section to the target buffer.
    fn finalize(self, target: &mut AlignedByteVec);

    /// Reads the metadata section from the buffer.
    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])>
    where
        Self: Sized;
}

/// Encoding empty metadata section whose purpose is to provide correct
/// aligmnet of the encoded data.
struct EmptyMetadata;

impl AlignedEncMetadata for EmptyMetadata {
    fn own_size() -> usize {
        0
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self
    }

    fn finalize(self, _target: &mut AlignedByteVec) {}

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(amudai_common::error::Error::invalid_format(
                "Encoded buffer size is too small",
            ));
        }
        Ok((Self, &buffer[Self::size()..]))
    }
}
