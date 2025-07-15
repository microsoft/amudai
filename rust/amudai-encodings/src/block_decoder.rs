use crate::encodings::EncodingPlan;
use amudai_sequence::sequence::ValueSequence;

/// Trait for decoding encoded data blocks back into sequences of values.
///
/// The `BlockDecoder` is the counterpart to `BlockEncoder` and is responsible for
/// reconstructing the original data from encoded blocks. It works with the encoded
/// binary data along with metadata to restore the values and their validity information.
///
/// # Usage
///
/// Block decoders are typically used in streaming scenarios where data needs to be
/// read back from storage or transmitted over a network. They handle the complexity
/// of different encoding schemes transparently.
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) to allow concurrent decoding
/// operations across multiple threads.
pub trait BlockDecoder: Send + Sync + 'static {
    /// Decodes a single block of encoded binary data into a sequence of values.
    ///
    /// # Arguments
    ///
    /// - `encoded` - A byte slice containing the encoded block, including the header
    ///   and the payload.
    /// - `value_count` - The number of values that are encoded in the block.
    ///
    /// # Returns
    ///
    /// A sequence of values, potentially accompanied by a validity (presence) bitmap.
    fn decode(&self, encoded: &[u8], value_count: usize) -> amudai_common::Result<ValueSequence>;

    /// Inspects the encoded block and returns the encoding plan used to encode it.
    ///
    /// # Arguments
    ///
    /// - `encoded` - A byte slice containing the encoded block, including the header
    ///   and the payload.
    ///
    /// # Returns
    ///
    /// The encoding plan used for the encoded block, or an error if the inspection fails.
    fn inspect(&self, encoded: &[u8]) -> amudai_common::Result<EncodingPlan>;
}
