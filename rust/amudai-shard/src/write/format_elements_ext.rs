use ahash::AHashSet;
use amudai_format::defs::{common::DataRef, shard};
use amudai_objectstore::url::ObjectUrl;

/// Extension methods for the `DataRef` element, useful when constructing
/// the data shard.
pub trait DataRefExt {
    /// Collects the URL of this `DataRef` into the specified set.
    fn collect_url(&self, set: &mut AHashSet<String>);

    /// Attempts to convert this `DataRef` into a relative URL, based
    /// on the provided `base_url`.
    fn try_make_relative(&mut self, base_url: Option<&ObjectUrl>);
}

impl DataRefExt for DataRef {
    fn collect_url(&self, set: &mut AHashSet<String>) {
        if !self.url.is_empty() && !set.contains(&self.url) {
            set.insert(self.url.clone());
        }
    }

    fn try_make_relative(&mut self, base_url: Option<&ObjectUrl>) {
        let Some(base_url) = base_url else {
            return;
        };
        if base_url.as_str() == self.url {
            self.url = String::new();
            return;
        }
        let Ok(url) = ObjectUrl::parse(&self.url) else {
            return;
        };
        if let Some(relative) = base_url.make_relative(&url) {
            self.url = relative;
        }
    }
}

/// Optimizes the data references (`DataRefs`) contained within the
/// storage format element.
pub trait CompactDataRefs {
    /// Gathers all unique URLs mentioned in this element into the specified set.
    fn collect_urls(&self, set: &mut AHashSet<String>);

    /// Tries to convert all `DataRefs` mentioned in this element into relative URLs,
    /// using the provided `base_url` as a reference.
    fn compact_data_refs(&mut self, shard_url: &ObjectUrl);
}

impl CompactDataRefs for shard::DataEncoding {
    fn collect_urls(&self, set: &mut AHashSet<String>) {
        let Some(ref kind) = self.kind else {
            return;
        };
        match kind {
            shard::data_encoding::Kind::Native(native) => {
                native.buffers.iter().for_each(|buf| buf.collect_urls(set));
            }
            shard::data_encoding::Kind::Parquet(_) => (),
        }
    }

    fn compact_data_refs(&mut self, shard_url: &ObjectUrl) {
        let Some(ref mut kind) = self.kind else {
            return;
        };
        match kind {
            shard::data_encoding::Kind::Native(native) => {
                native
                    .buffers
                    .iter_mut()
                    .for_each(|buf| buf.compact_data_refs(shard_url));
            }
            shard::data_encoding::Kind::Parquet(_) => (),
        }
    }
}

impl CompactDataRefs for shard::EncodedBuffer {
    fn collect_urls(&self, set: &mut AHashSet<String>) {
        [self.block_map.as_ref(), self.buffer.as_ref()]
            .into_iter()
            .flatten()
            .for_each(|data_ref| data_ref.collect_url(set));
    }

    fn compact_data_refs(&mut self, shard_url: &ObjectUrl) {
        [self.block_map.as_mut(), self.buffer.as_mut()]
            .into_iter()
            .flatten()
            .for_each(|data_ref| data_ref.try_make_relative(Some(shard_url)));
    }
}

#[cfg(test)]
mod tests {
    use amudai_format::defs::common::DataRef;
    use amudai_objectstore::url::ObjectUrl;

    use super::DataRefExt;

    #[test]
    fn test_make_relative_positive() {
        let base_url = ObjectUrl::parse("https://example.com/container/shard.shard").unwrap();
        let mut data_ref = DataRef::new("https://example.com/container/abcd", 0..10);
        data_ref.try_make_relative(Some(&base_url));
        assert_eq!(data_ref.url, "abcd");

        let mut data_ref = DataRef::new("https://example.com/container/abcd/efg", 0..10);
        data_ref.try_make_relative(Some(&base_url));
        assert_eq!(data_ref.url, "abcd/efg");
    }

    #[test]
    fn test_make_relative_same() {
        let base_url = ObjectUrl::parse("https://example.com/container/shard.shard").unwrap();
        let mut data_ref = DataRef::new("https://example.com/container/shard.shard", 0..10);
        data_ref.try_make_relative(Some(&base_url));
        assert_eq!(data_ref.url, "");
    }

    #[test]
    fn test_make_relative_negative() {
        let base_url = ObjectUrl::parse("https://example.com/container/shard.shard").unwrap();
        let mut data_ref = DataRef::new("https://example.com/container2/abcd", 0..10);
        data_ref.try_make_relative(Some(&base_url));
        assert_eq!(data_ref.url, "https://example.com/container2/abcd");
    }
}
