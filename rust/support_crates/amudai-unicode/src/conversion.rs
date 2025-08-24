const REPLACEMENT_CHAR_BYTES: [u8; 3] = [0xEF, 0xBF, 0xBD];

pub fn utf16_to_utf8<const LOSSY: bool, I: Iterator<Item = u16>>(
    input: I,
    target: &mut Vec<u8>,
) -> std::io::Result<()> {
    let decoded_chars = std::char::decode_utf16(input);
    let mut dst = [0u8; 4];
    if LOSSY {
        for ch in decoded_chars {
            if let Ok(ch) = ch {
                target.extend(ch.encode_utf8(&mut dst).as_bytes());
            } else {
                target.extend(&REPLACEMENT_CHAR_BYTES);
            }
        }
    } else {
        for ch in decoded_chars {
            let ch = ch.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            target.extend(ch.encode_utf8(&mut dst).as_bytes());
        }
    }
    Ok(())
}

pub fn utf32_to_utf8<const LOSSY: bool, I: Iterator<Item = u32>>(
    input: I,
    target: &mut Vec<u8>,
) -> std::io::Result<()> {
    let mut dst = [0u8; 4];
    if LOSSY {
        let decoded_chars = widestring::decode_utf32_lossy(input);
        for ch in decoded_chars {
            target.extend(ch.encode_utf8(&mut dst).as_bytes());
        }
    } else {
        let decoded_chars = widestring::decode_utf32(input);
        for ch in decoded_chars {
            let ch = ch.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            target.extend(ch.encode_utf8(&mut dst).as_bytes());
        }
    };
    Ok(())
}
