//! Extension traits for common IO primitives.

use std::borrow::Cow;

use crate::IoStream;

/// Extension trait providing a helper method to align writers that track their current position
/// by appending zero-filled padding.
pub trait AlignWrite {
    /// Aligns the writer to the specified byte alignment by writing zero-filled padding.
    ///
    /// This method calculates the number of padding bytes needed to make the current writer
    /// position a multiple of the given `alignment` and writes those bytes to the writer.
    ///
    /// # Arguments
    ///
    /// * `alignment`: The byte alignment to achieve.  Must be a power of 2.
    ///
    /// # Returns
    ///
    /// The number of padding bytes written.
    fn align(&mut self, alignment: usize) -> std::io::Result<usize>;
}

impl<W> AlignWrite for W
where
    W: ?Sized + IoStream,
{
    fn align(&mut self, alignment: usize) -> std::io::Result<usize> {
        let pos = self.current_size();
        let padding = get_padding(pos, alignment);
        self.write_all(&padding)?;
        Ok(padding.len())
    }
}

/// Returns a slice of padding bytes (all zeros) to align a given position to a specified
/// alignment.
///
/// This function calculates the necessary padding size and returns a `Cow<'static, [u8]>`
/// containing the padding bytes.  It attempts to borrow from a static buffer for smaller
/// padding sizes, and allocates a new `Vec` for larger sizes to avoid unnecessary allocations.
///
/// # Arguments
///
/// * `pos`: The current position (offset) as a `u64`.
/// * `alignment`: The desired alignment as a `usize`. Must be a power of two.
///
/// # Returns
///
/// A `Cow<'static, [u8]>` containing the padding bytes.  The returned slice will contain only
/// zero bytes.
pub fn get_padding(pos: u64, alignment: usize) -> Cow<'static, [u8]> {
    const BUF: [u8; 128] = [0u8; 128];
    let size = get_padding_size(pos, alignment);
    if size <= BUF.len() {
        Cow::Borrowed(&BUF[..size])
    } else {
        Cow::Owned(vec![0u8; size])
    }
}

/// Calculates the number of padding bytes needed to align a given position to a specified
/// alignment.
///
/// This function determines the number of zero bytes required to advance a given position
/// to the next aligned address. The alignment must be a power of two.
///
/// # Arguments
///
/// * `pos`: The current position (offset) as a `u64`.
/// * `alignment`: The desired alignment as a `usize`. Must be a power of two.
///
/// # Returns
///
/// The number of padding bytes required as a `usize`.
pub fn get_padding_size(pos: u64, alignment: usize) -> usize {
    let alignment = alignment.max(1);
    assert!(alignment.is_power_of_two());
    let next_pos = pos.checked_add(alignment as u64 - 1).expect("add") & !(alignment as u64 - 1);
    (next_pos - pos) as usize
}
