//! URL manipulation routines for resolving shard artifacts in the `ObjectStore`.
//!
//! Generally, the functions in this module require all URLs to be valid, clean,
//! sanitized and canonicalized. They will return an error if these conditions
//! are not met.
//! Specifically:
//! - URLs must not contain any credentials or sensitive information, whether in the
//!   authority component, the query component, or any custom suffix.
//! - URLs must not include a query component.
//! - URLs must not include a fragment component.
//! - URLs must not contain any path traversal sequences.
//! - URLs should be absolute unless a specific function explicitly allows a relative
//!   path.
//! - Absolute URLs should adhere to the following format:
//!   `scheme ":" ["//" authority] path`.
//!
//! A common convention is that a URL with a non-empty path component ending in a slash ("/")
//! represents a "container" or "folder." Conversely, the last component of a path without
//! a trailing slash represents an artifact within a container.

use std::borrow::Cow;

use url::Url;

macro_rules! verify {
    ($expr:expr) => {{
        let result = $expr;
        verify(result, stringify!($expr), None, None)?;
    }};

    ($expr:expr, $url:expr) => {{
        let result = $expr;
        verify(result, stringify!($expr), Some(&$url), None)?;
    }};

    ($expr:expr, $url:expr, $relative:expr) => {{
        let result = $expr;
        verify(result, stringify!($expr), Some(&$url), Some(&$relative))?;
    }};
}

/// Represents a URL that has been parsed and verified according to the `ObjectStore`
/// rules and conventions.
/// This URL is deemed "trusted" for further manipulations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectUrl(Url);

impl ObjectUrl {
    // Creates a new `ObjectUrl` from a `Url` after verifying it.
    ///
    /// Returns an error if the URL is invalid according to `ObjectStore` rules.
    pub fn new(url: Url) -> amudai_common::Result<ObjectUrl> {
        Self::verify_url(&url)?;
        Ok(Self(url))
    }

    /// Parses a string into a `ObjectUrl` after verifying it.
    ///
    /// Returns an error if the string is not a valid URL or if the URL is invalid
    /// according to `ObjectStore` rules.
    pub fn parse(url_str: &str) -> amudai_common::Result<ObjectUrl> {
        let url = parse_url(url_str)?;
        Self::verify_url(&url)?;
        // Ensure that the parsed and "reassembled" url is the same as the input.
        // This blocks non-canonical forms and path traversals.
        verify!(url.as_str() == url_str);
        Ok(Self(url))
    }

    /// Returns the URL as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Consumes the `ObjectUrl` and returns the inner `Url`.
    pub fn into_inner(self) -> Url {
        self.0
    }

    /// Resolves a relative path against this URL and ensures it does not escape the base directory.
    ///
    /// # Arguments
    ///
    /// * `rel_path` - A string slice that holds the relative path.
    ///
    /// # Returns
    ///
    /// * `Ok(ObjectUrl)` containing the absolute URL if successful.
    /// * `Err(...)` containing an error message if the URL is invalid or the relative path escapes
    ///   the base directory.
    pub fn resolve_relative(&self, rel_path: RelativePath) -> amudai_common::Result<ObjectUrl> {
        verify!(!rel_path.is_empty());
        let container = self.get_container()?;
        // Resolve the relative path against the base directory
        let resolved = container.join(&rel_path).map_err(|_e| {
            make_err(
                "Failed to join relative path",
                Some(self.as_str()),
                Some(&rel_path),
            )
        })?;
        // Ensure the resolved URL is within the base container.
        verify!(resolved.as_str().starts_with(container.as_str()));
        Ok(ObjectUrl(resolved))
    }

    /// Attempts to make a given `ObjectUrl` relative to this `ObjectUrl`.
    ///
    /// Returns `Some(String)` containing the relative path if successful, or `None`
    /// if the given URL is not within the same container or if the given URL
    /// is a container.
    pub fn make_relative(&self, url: &ObjectUrl) -> Option<String> {
        if url.is_container() {
            return None;
        }

        let container = self.get_container().ok()?;
        if url.as_str().starts_with(container.as_str()) {
            container.0.make_relative(url)
        } else {
            None
        }
    }

    /// Determines if the specified `url` refers to an object location
    /// that is confined within the container of this URL.
    pub fn has_in_scope(&self, url: &ObjectUrl) -> bool {
        let Ok(container) = self.get_container() else {
            return false;
        };
        url.as_str().starts_with(container.as_str())
    }

    /// Returns the container URL for this URL.
    ///
    /// If the URL is already a container, returns a borrowed reference to self.
    /// Otherwise, returns a new `ObjectUrl` representing the parent container.
    pub fn get_container(&self) -> amudai_common::Result<Cow<'_, ObjectUrl>> {
        if self.path().ends_with('/') {
            Ok(Cow::Borrowed(self))
        } else {
            let parent_url = self
                .join("./")
                .map_err(|_e| make_err("failed to determine parent", Some(self.as_str()), None))?;
            Ok(Cow::Owned(ObjectUrl(parent_url)))
        }
    }

    /// Checks if the URL represents a container (i.e., ends with a `/`).
    pub fn is_container(&self) -> bool {
        self.path().ends_with('/')
    }

    /// Verifies that the given `Url` is valid according to `ObjectStore` rules.
    ///
    /// Returns an error if the URL is invalid.
    pub fn verify_url(url: &Url) -> amudai_common::Result<()> {
        verify!(url.username().is_empty());
        verify!(url.password().is_none());
        verify!(url.query().is_none());
        verify!(url.fragment().is_none());
        verify!(url.path_segments().is_some());
        verify!(url.path().starts_with('/'));
        Ok(())
    }
}

impl std::ops::Deref for ObjectUrl {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&str> for ObjectUrl {
    type Error = amudai_common::error::Error;

    fn try_from(url_str: &str) -> Result<Self, Self::Error> {
        ObjectUrl::parse(url_str)
    }
}

/// Represents a relative path that has been verified to not contain
/// any path traversal sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelativePath<'a>(&'a str);

impl<'a> RelativePath<'a> {
    /// Creates a new `RelativePath` from a string slice after verifying it.
    ///
    /// Returns an error if the path starts with a `/` or contains path traversal
    /// sequences.
    pub fn new(path: &'a str) -> amudai_common::Result<RelativePath<'a>> {
        verify!(Self::is_valid(path));
        Ok(RelativePath(path))
    }

    pub fn is_valid(s: &str) -> bool {
        !s.starts_with('/')
            && s.split('/').all(Self::is_valid_segment)
            && !Self::starts_with_scheme(s)
    }

    /// Checks if a path segment is valid (i.e., does not contain path traversal sequences).
    fn is_valid_segment(segment: &str) -> bool {
        !matches!(
            segment,
            ".." | "%2e%2e"
                | "%2e%2E"
                | "%2E%2e"
                | "%2E%2E"
                | "%2e."
                | "%2E."
                | ".%2e"
                | ".%2E"
                | "."
                | "%2e"
                | "%2E"
        )
    }

    /// Determines whether a given path begins with a valid URL scheme, indicating that
    /// it cannot be considered a valid relative path.
    fn starts_with_scheme(s: &str) -> bool {
        if s.is_empty() {
            return false;
        }

        let mut scheme_end_pos = None;
        for (index, char) in s.char_indices() {
            if index == 0 {
                if !char.is_ascii_alphabetic() {
                    return false;
                }
            } else if char == ':' {
                scheme_end_pos = Some(index);
                break;
            } else if !char.is_ascii_alphanumeric() && char != '+' && char != '-' && char != '.' {
                return false;
            }
        }

        scheme_end_pos.is_some_and(|i| i > 0)
    }
}

impl std::ops::Deref for RelativePath<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

/// Parses a string into a `Url`.
///
/// Returns an error if the string is not a valid URL.
fn parse_url(url: &str) -> amudai_common::Result<Url> {
    Url::parse(url).map_err(|e| {
        amudai_common::error::ErrorKind::ResolveUrl {
            url: url.to_string(),
            relative: None,
            reason: format!("Failed to parse url, error: {e}"),
        }
        .into()
    })
}

/// Verifies a predicate and returns an error if it is false.
fn verify(
    predicate: bool,
    condition: &str,
    url: Option<&str>,
    relative: Option<&str>,
) -> amudai_common::Result<()> {
    if predicate {
        Ok(())
    } else {
        Err(make_err(condition, url, relative))
    }
}

/// Creates a new `amudai_common::error::Error` with the given reason and optional
/// URL and relative path.
fn make_err(
    reason: &str,
    url: Option<&str>,
    relative: Option<&str>,
) -> amudai_common::error::Error {
    amudai_common::error::ErrorKind::ResolveUrl {
        url: url.map(String::from).unwrap_or_default(),
        relative: relative.map(String::from),
        reason: reason.to_string(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_within_directory() {
        let base = "https://www.example.com/container/shard";
        let relative = "index/terms";
        let expected = "https://www.example.com/container/index/terms";
        assert_eq!(resolve_url_str(base, relative).unwrap().as_str(), expected);
    }

    #[test]
    fn test_resolve_empty() {
        let base = "https://www.example.com/container/shard";
        let relative = "";
        assert!(resolve_url_str(base, relative).is_err());
    }

    #[test]
    fn test_resolve_within_directory_by_dot_slash() {
        let base = "https://www.example.com/container/shard";
        let relative = "./index/terms";
        assert!(resolve_url_str(base, relative).is_err());
    }

    #[test]
    fn test_resolve_within_directory_by_dot() {
        let base = "https://www.example.com/container/shard";
        let relative = ".";
        assert!(resolve_url_str(base, relative).is_err());
    }

    #[test]
    fn test_resolve_with_different_host() {
        let base = "https://www.example.com/container/shard";
        let relative = "https://www.example.org/container/shard";
        assert!(resolve_url_str(base, relative).is_err());
    }

    #[test]
    fn test_resolve_within_directory_trailing_slash() {
        let base = "https://www.example.com/container/";
        let relative = "stripe.bin";
        let expected = "https://www.example.com/container/stripe.bin";
        assert_eq!(resolve_url_str(base, relative).unwrap().as_str(), expected);
    }

    #[test]
    fn test_resolve_escaping_directory() {
        let base = "https://www.example.com/container/shard";
        let relative = "../test/image.jpg";
        assert!(resolve_url_str(base, relative).is_err());
    }

    #[test]
    fn test_resolve_escaping_directory_trailing_slash() {
        let base = "https://www.example.com/container1/folder1/";
        let relative = "../../image.jpg";
        assert!(resolve_url_str(base, relative).is_err());
    }

    #[test]
    fn test_resolve_complex_relative() {
        let base = "https://www.example.com/container/shard";
        let relative = "./index/../image.jpg";
        assert!(resolve_url_str(base, relative).is_err());
    }

    #[test]
    fn test_resolve_absolute_relative() {
        let base = "https://www.example.com/container/shard";
        let relative = "/container/image.jpg";
        assert!(resolve_url_str(base, relative).is_err());
    }

    #[test]
    fn test_resolve_absolute_traversal() {
        let base = "https://www.example.com/container/shard";
        let relative = "/container1/data/../../image.jpg";
        assert!(resolve_url_str(base, relative).is_err());
    }

    fn resolve_url_str(base_url: &str, relative: &str) -> amudai_common::Result<Url> {
        let base_url = ObjectUrl::parse(base_url)?;
        let rel_path = RelativePath::new(relative)?;
        Ok(base_url.resolve_relative(rel_path)?.clone().into_inner())
    }

    fn test_parse(url_str: &str) -> ObjectUrl {
        let res = ObjectUrl::parse(url_str);
        if res.is_err() {
            let parsed_res = Url::parse(url_str);
            assert!(parsed_res.is_ok(), "Failed to parse '{url_str}'");
            assert_eq!(url_str, parsed_res.unwrap().as_str());
        }
        res.unwrap()
    }

    #[test]
    fn test_parse_and_verify_url() {
        test_parse("https://foo.com/");
        test_parse("https://foo.com/aaa");
        test_parse("https://foo.com/aaa/");
        test_parse("https://foo.com/aaa/bcde");
        assert!(ObjectUrl::parse("file:test").is_err());
        assert!(ObjectUrl::parse("file:/test").is_err());
        assert!(ObjectUrl::parse("file:/test/").is_err());
        let url = test_parse("file:///test/");
        assert_eq!(url.path(), "/test/");
        let url = test_parse("file:///test/bcde");
        assert_eq!(url.path(), "/test/bcde");
        let url = test_parse("file://aaa/test/");
        assert_eq!(url.domain(), Some("aaa"));
        assert_eq!(url.host().unwrap().to_string(), "aaa");
        assert_eq!(url.path(), "/test/");
        let url = test_parse("file://127.0.0.1/test/");
        assert_eq!(url.host().unwrap().to_string(), "127.0.0.1");
        assert_eq!(url.path(), "/test/");
        test_parse("https://foo.com/aaa/%d1%81%d0%bb4%d1%84/bcde");

        assert!(
            ObjectUrl::parse("https://foo.com").is_err(),
            "should err on missing trailing slash"
        );
        assert!(ObjectUrl::parse("https://foo.com/aaa/%2e%2e/bcde").is_err());
        assert!(ObjectUrl::parse("https://foo.com/aaa/../bcde").is_err());
        assert!(ObjectUrl::parse("https://foo.com/aaa/./bcde").is_err());
        assert!(ObjectUrl::parse("https://foo.com/aaa/bcde?aaa").is_err());
        assert!(ObjectUrl::parse("https://user:p1@foo.com/aaa/bcde").is_err());
    }

    #[test]
    fn test_make_relative_same_object() {
        let base = ObjectUrl::parse("http://foo.com/c/aaa.shard").unwrap();
        let rel = base
            .make_relative(&ObjectUrl::parse("http://foo.com/c/aaa.shard").unwrap())
            .unwrap();
        assert_eq!(rel, "aaa.shard");
    }

    #[test]
    fn test_make_relative_same_container() {
        let base = ObjectUrl::parse("s3://bucket/container/").unwrap();
        let url = ObjectUrl::parse("s3://bucket/container/file.txt").unwrap();
        assert_eq!(base.make_relative(&url), Some("file.txt".to_string()));
    }

    #[test]
    fn test_make_relative_nested_container() {
        let base = ObjectUrl::parse("s3://bucket/container/").unwrap();
        let url = ObjectUrl::parse("s3://bucket/container/nested/file.txt").unwrap();
        assert_eq!(
            base.make_relative(&url),
            Some("nested/file.txt".to_string())
        );
    }

    #[test]
    fn test_make_relative_nested_container_with_trailing_slash() {
        let base = ObjectUrl::parse("s3://bucket/container/").unwrap();
        let url = ObjectUrl::parse("s3://bucket/container/nested/").unwrap();
        assert_eq!(base.make_relative(&url), None);
    }

    #[test]
    fn test_make_relative_different_container() {
        let base = ObjectUrl::parse("s3://bucket/container1/").unwrap();
        let url = ObjectUrl::parse("s3://bucket/container2/file.txt").unwrap();
        assert_eq!(base.make_relative(&url), None);
    }

    #[test]
    fn test_make_relative_different_bucket() {
        let base = ObjectUrl::parse("s3://bucket1/container/").unwrap();
        let url = ObjectUrl::parse("s3://bucket2/container/file.txt").unwrap();
        assert_eq!(base.make_relative(&url), None);
    }

    #[test]
    fn test_make_relative_base_is_file() {
        let base = ObjectUrl::parse("s3://bucket/container/file.txt").unwrap();
        let url = ObjectUrl::parse("s3://bucket/container/file2.txt").unwrap();
        assert_eq!(base.make_relative(&url).unwrap(), "file2.txt");
    }

    #[test]
    fn test_make_relative_url_is_container() {
        let base = ObjectUrl::parse("s3://bucket/container/").unwrap();
        let url = ObjectUrl::parse("s3://bucket/container/nested/").unwrap();
        assert_eq!(base.make_relative(&url), None);
    }

    #[test]
    fn test_make_relative_base_is_root() {
        let base = ObjectUrl::parse("s3://bucket/").unwrap();
        let url = ObjectUrl::parse("s3://bucket/file.txt").unwrap();
        assert_eq!(base.make_relative(&url), Some("file.txt".to_string()));
    }

    #[test]
    fn test_make_relative_base_is_root_nested() {
        let base = ObjectUrl::parse("s3://bucket/").unwrap();
        let url = ObjectUrl::parse("s3://bucket/nested/file.txt").unwrap();
        assert_eq!(
            base.make_relative(&url),
            Some("nested/file.txt".to_string())
        );
    }

    #[test]
    fn test_make_relative_base_is_root_nested_container() {
        let base = ObjectUrl::parse("s3://bucket/").unwrap();
        let url = ObjectUrl::parse("s3://bucket/nested/").unwrap();
        assert_eq!(base.make_relative(&url), None);
    }

    #[test]
    fn test_try_from() {
        fn to_url(
            url: impl TryInto<ObjectUrl, Error: Into<amudai_common::error::Error>>,
        ) -> amudai_common::Result<ObjectUrl> {
            url.try_into().map_err(|e| e.into())
        }

        assert!(to_url("file:///aaa/bbb").is_ok());
        assert!(to_url("sdgd").is_err());
        assert!(to_url(ObjectUrl::parse("file:///aaa/bbb").unwrap()).is_ok());
    }
}
