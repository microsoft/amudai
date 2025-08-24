use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryInto;
use std::io;
use std::mem::MaybeUninit;
use uuid::Uuid;
use uuid_simd::UuidExt;

/// 16-byte GUID, little-endian layout compatible with .NET Guid struct.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(from = "uuid::Uuid", into = "uuid::Uuid")]
#[repr(C)]
pub struct Guid([u8; 16]);

impl Guid {
    /// Generates a new random (v4) GUID.
    pub fn new() -> Guid {
        uuid::Uuid::new_v4().into()
    }

    /// Alias for `new()` returning a random v4 GUID.
    pub fn new_v4() -> Guid {
        Guid::new()
    }

    /// Returns the NIL (all zero) GUID.
    pub const fn nil() -> Guid {
        Guid([0; 16])
    }

    /// Parses a GUID from a canonical or hyphen-less hexadecimal string.
    pub fn parse_str(input: &str) -> Result<Guid, io::Error> {
        uuid::Uuid::parse_str(input)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
            .map(|u| u.into())
    }

    /// Loosely parses a UUID present at the start of the input, returning the GUID and consumed length.
    ///
    /// Supported UUID formats are:
    ///  - Optionally enclosed into curly braces or square brackets.
    ///  - With or without hyphens (between any group).
    ///
    /// # Returns
    /// On success, the method returns `Ok` containing a tuple:
    /// (parsed UUID, parsed string length)
    ///
    /// # Examples
    /// ```
    /// use amudai_guid::Guid;
    ///
    /// assert!(Guid::parse_str_prefix("d1cfbf4a-829c-43f3-971e").is_err());
    /// assert!(Guid::parse_str_prefix("d1cfbf4a829c43f3971e").is_err());
    ///
    /// let g = Guid::parse_str("d1cfbf4a-829c-43f3-971e-2795639de014").unwrap();
    /// assert_eq!(Guid::parse_str_prefix("d1cfbf4a829c43f3971e2795639de014").unwrap(), (g, 32));
    /// assert_eq!(Guid::parse_str_prefix("d1cfbf4a-829c-43f3-971e-2795639de014").unwrap(), (g, 36));
    /// assert_eq!(Guid::parse_str_prefix("d1cfbf4a-829c-43f3-971e2795639de014").unwrap(), (g, 35));
    /// assert_eq!(Guid::parse_str_prefix("{d1cfbf4a829c43f3971e2795639de014}").unwrap(), (g, 34));
    /// assert_eq!(Guid::parse_str_prefix("{d1cfbf4a-829c-43f3-971e-2795639de014}").unwrap(), (g, 38));
    /// assert_eq!(Guid::parse_str_prefix("[d1cfbf4a-829c-43f3-971e-2795639de014]").unwrap(), (g, 38));
    /// assert_eq!(Guid::parse_str_prefix("d1cfbf4a-829c-43f3-971e-2795639de014*abc").unwrap(), (g, 36));
    /// ```
    pub fn parse_str_prefix(input: &str) -> Result<(Guid, usize), io::Error> {
        let len = input.len();
        if len < 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UUID is too short",
            ));
        }

        let mut buf = [0u8; 16];
        let s = input.as_bytes();
        let (s, skip_brace) = if s[0] == b'{' || s[0] == b'[' {
            (&s[1..], true)
        } else {
            (s, false)
        };

        buf[3] = byte_from_hex_str(&s[0..2])?;
        buf[2] = byte_from_hex_str(&s[2..4])?;
        buf[1] = byte_from_hex_str(&s[4..6])?;
        buf[0] = byte_from_hex_str(&s[6..8])?;
        let s = if s[8] == b'-' { &s[9..] } else { &s[8..] };

        buf[5] = byte_from_hex_str(&s[0..2])?;
        buf[4] = byte_from_hex_str(&s[2..4])?;
        let s = if s[4] == b'-' { &s[5..] } else { &s[4..] };

        buf[7] = byte_from_hex_str(&s[0..2])?;
        buf[6] = byte_from_hex_str(&s[2..4])?;
        let s = if s[4] == b'-' { &s[5..] } else { &s[4..] };

        buf[8] = byte_from_hex_str(&s[0..2])?;
        buf[9] = byte_from_hex_str(&s[2..4])?;
        let s = if s[4] == b'-' { &s[5..] } else { &s[4..] };

        if s.len() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UUID string is too short",
            ));
        }

        buf[10] = byte_from_hex_str(&s[0..2])?;
        buf[11] = byte_from_hex_str(&s[2..4])?;
        buf[12] = byte_from_hex_str(&s[4..6])?;
        buf[13] = byte_from_hex_str(&s[6..8])?;
        buf[14] = byte_from_hex_str(&s[8..10])?;
        buf[15] = byte_from_hex_str(&s[10..12])?;
        let s = &s[12..];

        let s = if skip_brace {
            if s.is_empty() || (s[0] != b']' && s[0] != b'}') {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unclosed braces in UUID string",
                ));
            }
            &s[1..]
        } else {
            s
        };

        Ok((Self::from_bytes(&buf), input.len() - s.len()))
    }

    fn uuid_toguid(slice: &mut [u8; 16], uuid: Uuid) {
        //	"d1cfbf4a-829c-43f3-971e-2795639de014"	string
        // {4abfcfd1-9c82-f343-971e-2795639de014}	UUID
        // {d1cfbf4a-829c-43f3-971e-2795639de014}	Guid
        let bytes = uuid.as_bytes();
        slice[3] = bytes[0];
        slice[2] = bytes[1];
        slice[1] = bytes[2];
        slice[0] = bytes[3];
        slice[5] = bytes[4];
        slice[4] = bytes[5];
        slice[7] = bytes[6];
        slice[6] = bytes[7];
        slice[8..].copy_from_slice(&bytes[8..]);
    }

    /// Attempts to parse a GUID (with optional surrounding braces/brackets) returning the GUID and consumed length.
    pub fn parse_guid_opt(input: &str) -> Result<(Guid, usize), io::Error> {
        let mut buf = [0u8; 16];
        let mut len: usize = 0;
        let uuid = UuidExt::parse(input);
        match uuid {
            Ok(uuid) => {
                Self::uuid_toguid(&mut buf, uuid);
                Ok((Guid(buf), input.len()))
            }
            Err(e) => {
                let mut uuid = Err(e);
                if input.len() >= 34 {
                    let bytes = input.as_bytes();
                    if let [b'{', s @ .., b'}'] = bytes {
                        uuid = UuidExt::parse(s);
                        len = s.len() + 2;
                    }
                    if let [b'[', s @ .., b']'] = bytes {
                        uuid = UuidExt::parse(s);
                        len = s.len() + 2;
                    }
                }
                match uuid {
                    Ok(uuid) => {
                        Self::uuid_toguid(&mut buf, uuid);
                        Ok((Guid(buf), len))
                    }
                    Err(_) => match Guid::parse_str_prefix(input) {
                        Ok((guid, len)) => Ok((guid, len)),
                        _ => Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Not a valid GUID string",
                        )),
                    },
                }
            }
        }
    }

    /// Constructs a GUID from individual field components (a, b, c, d).
    pub fn from_fields(a: u32, b: u16, c: u16, d: &[u8; 8]) -> Guid {
        Self::from_guid_fields(GuidFields { a, b, c, d: *d })
    }

    /// Decomposes the GUID into its field components.
    pub fn to_fields(&self) -> (u32, u16, u16, [u8; 8]) {
        let GuidFields { a, b, c, d } = self.to_guid_fields();
        (a, b, c, d)
    }

    /// Returns the GUID as a pair of little-endian u64 values.
    pub fn to_u64_pair(&self) -> (u64, u64) {
        let v0 = u64::from_le_bytes(self.as_slice()[..8].try_into().expect("to_u64_pair"));
        let v1 = u64::from_le_bytes(self.as_slice()[8..].try_into().expect("to_u64_pair"));
        (v0, v1)
    }

    /// Creates a GUID by copying from a 16-byte array.
    pub fn from_bytes(bytes: &[u8; 16]) -> Guid {
        Guid(*bytes)
    }

    /// Returns a reference to the underlying 16 raw bytes.
    pub const fn bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Builds a GUID from a 16-byte slice, validating length.
    pub fn from_slice(s: &[u8]) -> io::Result<Guid> {
        Ok(Guid(s.try_into().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidInput, e)
        })?))
    }

    /// Returns the underlying bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns true if this GUID equals the NIL GUID.
    pub fn is_nil(&self) -> bool {
        *self == Guid::nil()
    }

    /// Formats this GUID into the provided buffer, returning the number of bytes written.
    pub fn format_to_buffer(&self, buf: &mut [u8]) -> io::Result<usize> {
        use std::io::Write;
        let mut c = io::Cursor::new(buf);
        write!(&mut c, "{self}")?;
        Ok(c.position() as usize)
    }

    /// Lexicographically compares this GUID to another using field order (a, b, c, d).
    pub fn compare(&self, other: &Guid) -> Ordering {
        let self_parts = self.to_guid_fields();
        let other_parts = other.to_guid_fields();

        if self_parts.a != other_parts.a {
            return self_parts.a.cmp(&other_parts.a);
        }
        if self_parts.b != other_parts.b {
            return self_parts.b.cmp(&other_parts.b);
        }
        if self_parts.c != other_parts.c {
            return self_parts.c.cmp(&other_parts.c);
        }
        if self_parts.d != other_parts.d {
            return self_parts.d.cmp(&other_parts.d);
        }
        Ordering::Equal
    }

    fn from_guid_fields(fields: GuidFields) -> Guid {
        let mut guid = MaybeUninit::<Guid>::uninit();
        unsafe {
            std::ptr::copy_nonoverlapping(
                &fields as *const GuidFields as *const u8,
                guid.as_mut_ptr() as _,
                std::mem::size_of::<Guid>(),
            );
            guid.assume_init()
        }
    }

    fn to_guid_fields(self) -> GuidFields {
        let mut fields = MaybeUninit::<GuidFields>::uninit();
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.0.as_ptr(),
                fields.as_mut_ptr() as _,
                std::mem::size_of::<Guid>(),
            );
            fields.assume_init()
        }
    }
}

impl std::fmt::Debug for Guid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        uuid::Uuid::from(*self).fmt(f)
    }
}

#[inline]
/// Parses exactly two hexadecimal characters as byte,
/// without performing any bounds checking.
fn byte_from_hex_str(s: &[u8]) -> Result<u8, io::Error> {
    let mut acc = 0;
    for &ch in &s[..2] {
        acc = acc * 16
            + match ch {
                b'0'..=b'9' => ch - b'0',
                b'a'..=b'f' => ch - b'a' + 10,
                b'A'..=b'F' => ch - b'A' + 10,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid characters in UUID",
                    ));
                }
            };
    }
    Ok(acc)
}

impl std::fmt::Display for Guid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        uuid::Uuid::from(*self).fmt(f)
    }
}

impl From<uuid::Uuid> for Guid {
    fn from(u: uuid::Uuid) -> Self {
        let (a, b, c, d) = u.as_fields();
        Guid::from_fields(a, b, c, d)
    }
}

impl From<Guid> for uuid::Uuid {
    fn from(g: Guid) -> Self {
        let GuidFields { a, b, c, d } = g.to_guid_fields();
        uuid::Uuid::from_fields(a, b, c, &d)
    }
}

impl AsRef<[u8]> for Guid {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::str::FromStr for Guid {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Guid::parse_str(s)
    }
}

impl Default for Guid {
    #[inline]
    fn default() -> Self {
        Guid::nil()
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(C)]
struct GuidFields {
    a: u32,
    b: u16,
    c: u16,
    d: [u8; 8],
}

#[cfg(test)]
mod tests {
    use super::Guid;

    #[test]
    fn test_guid_representation() {
        assert_eq!(std::mem::size_of::<Guid>(), 16);
        assert_eq!(std::mem::size_of::<uuid::Uuid>(), 16);

        let g = Guid::new();
        let u = uuid::Uuid::from(g);
        assert_eq!(g.to_string(), u.to_string());

        let s = "e649d435-85c3-4349-a579-d9d513f8e4f5";
        let g = Guid::parse_str(s).unwrap();
        assert_eq!(g.to_string(), s);
        let u = uuid::Uuid::from(g);
        assert_eq!(u.to_string(), s);
        let u = uuid::Uuid::parse_str(s).unwrap();
        assert_eq!(u.to_string(), g.to_string());

        let g = Guid::parse_str("01020304-0506-0708-1020-304050607080").unwrap();
        assert_eq!(
            g.as_slice(),
            &[
                0x04, 0x03, 0x02, 0x01, 0x06, 0x05, 0x08, 0x07, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60,
                0x70, 0x80
            ]
        );
    }

    #[test]
    fn test_parse_str_prefix() {
        let g = Guid::parse_str("d1cfbf4a-829c-43f3-971e-2795639de014").unwrap();

        for s in [
            "d1cfbf4a-829c-43f3-971e",
            "d1cfbf4a829c43f3971e",
            "d1cfbf4a-829c-43f3-971e-2795639de01",
            "{d1cfbf4a-829c-43f3-971e-2795639de014",
            "d1-cfbf4a-829c-43f3-971e-2795639de014",
            "d1cfbf4a-829c-43f3-971e-2795639de-014",
        ] {
            assert!(Guid::parse_str_prefix(s).is_err());
        }

        for (s, r) in vec![
            ("d1cfbf4a829c43f3971e2795639de014", (g, 32)),
            ("d1cfbf4a-829c-43f3-971e-2795639de014", (g, 36)),
            ("d1cfbf4a-829c-43f3-971e2795639de014", (g, 35)),
            ("{d1cfbf4a829c43f3971e2795639de014}", (g, 34)),
            ("{d1cfbf4a-829c-43f3-971e-2795639de014}", (g, 38)),
            ("{D1CFBF4A-829C-43F3-971E-2795639DE014}", (g, 38)),
            ("{d1cfbf4a-829c-43f3-971e-2795639de014]", (g, 38)),
            ("[d1cfbf4a-829c-43f3-971e-2795639de014]", (g, 38)),
            ("{d1cfbf4a-829c-43f3-971e-2795639de014]", (g, 38)),
            ("d1cfbf4a-829c-43f3-971e-2795639de014*abc", (g, 36)),
            ("d1cfbf4a-829c-43f3-971e-2795639de014567", (g, 36)),
        ] {
            assert_eq!(Guid::parse_str_prefix(s).unwrap(), r);
        }
    }

    #[test]
    fn test_guid_compare() {
        // fields:                          "aaaaaaaa-bbbb-cccc-dddd-dddddddddddd"
        let g1 = Guid::parse_str("01020304-0506-0708-1020-304050607080").unwrap();
        let mut g2 = Guid::parse_str("01020304-0506-0708-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Equal);

        // compare a
        g2 = Guid::parse_str("01020305-0506-0708-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020303-0506-0708-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        // compare b
        g2 = Guid::parse_str("01020304-0507-0708-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020304-0505-0708-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        // compare c
        g2 = Guid::parse_str("01020304-0506-0709-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020304-0506-0706-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        // compare d
        g2 = Guid::parse_str("01020304-0506-0708-1021-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020304-0506-0708-1010-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        g2 = Guid::parse_str("01020304-0506-0708-1020-304050607081").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020304-0506-0708-1020-304050607070").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        // order of parts cmp
        g2 = Guid::parse_str("01020305-0000-0000-0000-000000000000").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020303-ffff-ffff-ffff-ffffffffffff").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        g2 = Guid::parse_str("01020304-0507-0708-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020304-0505-0708-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        g2 = Guid::parse_str("01020304-0506-0709-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020304-0506-0707-1020-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        g2 = Guid::parse_str("01020304-0506-0708-1021-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020304-0506-0708-101f-304050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);

        g2 = Guid::parse_str("01020304-0506-0708-1020-404050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Less);
        g2 = Guid::parse_str("01020304-0506-0708-1020-204050607080").unwrap();
        assert_eq!(g1.compare(&g2), std::cmp::Ordering::Greater);
    }
}
