#[macro_export]
macro_rules! verify {
    ($expr:expr) => {{
        let result = $expr;
        $crate::utils::verify(result, stringify!($expr))?;
    }};
}

pub fn verify(predicate: bool, condition: &str) -> std::io::Result<()> {
    if predicate {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            condition,
        ))
    }
}

pub fn read_fully<R: std::io::Read>(mut read: R, buffer: &mut [u8]) -> std::io::Result<usize> {
    let mut pos: usize = 0;
    loop {
        let r = read.read(&mut buffer[pos..]);
        match r {
            Ok(0) => return Ok(pos),
            Ok(bytes) => {
                pos += bytes;
                if pos == buffer.len() {
                    return Ok(pos);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}
