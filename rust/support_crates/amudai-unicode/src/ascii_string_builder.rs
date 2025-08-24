use itoa;
use std::char::from_digit;
use std::io::{Error, ErrorKind, Result, Write};

/// AsciiStringBuilder is a string formatter which is initialized with a buffer
/// of fixed size and can be used to append Ascii characters/numbers to the end of the buffer.
/// this builder is more performant than write!() with formatted string because using this builder
/// one can append chars/integers directly avoiding the penalty of going through fmt::Formatter.
pub struct AsciiStringBuilder<'a> {
    /// Buffer with fixed size which will be used to build the string.
    buffer: &'a mut [u8],
    /// Pointer to the end of the data appended so far.
    position: usize,
}

impl<'a> AsciiStringBuilder<'a> {
    pub fn new(arg_buffer: &'a mut [u8]) -> AsciiStringBuilder<'a> {
        AsciiStringBuilder {
            buffer: arg_buffer,
            position: 0usize,
        }
    }

    /// Helping method which generates buffer too small error with current empty space and required space.
    fn buffer_too_small(&mut self, required_space: i64) -> String {
        format!(
            "Buffer is too small. empty space = {}, required space = {}",
            self.buffer.len() - self.position,
            required_space
        )
    }

    fn number_has_too_much_digits(
        &mut self,
        expected_digits_number: usize,
        actual_number: i64,
    ) -> String {
        format!(
            "Expected for number of {expected_digits_number} digits but received {actual_number}"
        )
    }

    /// Given an integer, write it to the end of buffer.
    pub fn append_i64_as_str(&mut self, val: i64) -> Result<usize> {
        let mut buffer = itoa::Buffer::new();
        let s = buffer.format(val);
        let bytes = s.as_bytes();
        let required_space = bytes.len();

        if self.position + required_space > self.buffer.len() {
            self.position = 0;
            return Err(Error::new(
                ErrorKind::InvalidInput,
                self.buffer_too_small(required_space as i64),
            ));
        }

        self.buffer[self.position..self.position + required_space].copy_from_slice(bytes);
        self.position += required_space;
        Ok(required_space)
    }

    /// safely appends an integer as n digits.
    /// if the number has less than n digits, it will append 0 to the beginning so appending 2 as 2 digits will be added as "02"
    /// if the number has more digits (which means we lost some digits), the function fails.
    pub fn safe_append_n_digits_number(&mut self, val: i64, digits: usize) -> Result<usize> {
        if self.position + (digits - 1) >= self.buffer.len() {
            self.position = 0;
            return Err(invalid_arg(&self.buffer_too_small(2)));
        }
        let mut remained_digits = digits;
        let mut current_val = val;
        while remained_digits > 0 {
            let d = from_digit((current_val % 10) as u32, 10);
            match d {
                None => {
                    self.position = 0;
                    return Err(invalid_arg("Failed to read digit"));
                }
                Some(d) => {
                    self.buffer[self.position + remained_digits - 1] = d as u8;
                    current_val /= 10;
                    remained_digits -= 1;
                }
            }
        }
        if current_val != 0 {
            self.position = 0;
            return Err(invalid_arg(&self.number_has_too_much_digits(digits, val)));
        }
        self.position += digits;
        Ok(digits)
    }

    /// Given an ascii char, append it to the end of the buffer.
    pub fn append_ascii_character(&mut self, u: u8) -> Result<usize> {
        if self.position >= self.buffer.len() {
            self.position = 0;
            return Err(invalid_arg(&self.buffer_too_small(1)));
        }
        self.buffer[self.position] = u;
        self.position += 1;
        Ok(1)
    }

    pub fn write_str_slice(&mut self, str_slice: &[u8]) -> Result<usize> {
        let len = str_slice.len();
        if len + self.position > self.buffer.len() {
            return Err(invalid_arg(&self.buffer_too_small(len as i64)));
        }

        match (&mut self.buffer[self.position..]).write(str_slice) {
            Ok(written_bytes) => {
                self.position += written_bytes;
                Ok(written_bytes)
            }
            Err(e) => {
                self.position = 0;
                Err(e)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.position
    }

    pub fn is_empty(&self) -> bool {
        self.position == 0
    }

    /// Get the slice of data written so far.
    pub fn get_slice(&mut self) -> &mut [u8] {
        &mut self.buffer[0..self.position]
    }
}

fn invalid_arg(name: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_i64_as_str() {
        let mut buffer = [0u8; 32];
        let mut builder = AsciiStringBuilder::new(&mut buffer);

        // Test positive number
        let result = builder.append_i64_as_str(12345);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);
        assert_eq!(std::str::from_utf8(&buffer[0..5]).unwrap(), "12345");

        // Test negative number
        let mut buffer2 = [0u8; 32];
        let mut builder2 = AsciiStringBuilder::new(&mut buffer2);
        let result2 = builder2.append_i64_as_str(-6789);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), 5);
        assert_eq!(std::str::from_utf8(&buffer2[0..5]).unwrap(), "-6789");

        // Test zero
        let mut buffer3 = [0u8; 32];
        let mut builder3 = AsciiStringBuilder::new(&mut buffer3);
        let result3 = builder3.append_i64_as_str(0);
        assert!(result3.is_ok());
        assert_eq!(result3.unwrap(), 1);
        assert_eq!(std::str::from_utf8(&buffer3[0..1]).unwrap(), "0");
    }

    #[test]
    fn test_append_i64_as_str_buffer_too_small() {
        let mut buffer = [0u8; 2]; // Too small for larger numbers
        let mut builder = AsciiStringBuilder::new(&mut buffer);

        let result = builder.append_i64_as_str(12345); // 5 chars but buffer is only 2
        assert!(result.is_err());
    }
}
