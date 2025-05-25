use std::sync::Arc;

use amudai_objectstore::url::ObjectUrl;

/// `AnchoredElement<T>` encapsulates a storage format element `T`, associating it
/// with an "anchor" - a URL of the artifact from which this element was loaded.
///
/// The anchor is utilized when resolving empty `DataRefs` originating from the element.
pub struct AnchoredElement<T> {
    /// The storage format element, loaded from the artifact.
    inner: T,
    /// The artifact containing the element. Often, this is the shard directory.
    anchor: Arc<ObjectUrl>,
}

impl<T> AnchoredElement<T> {
    /// Constructs a new `AnchoredElement` with the given element and anchor.
    pub fn new(inner: T, anchor: Arc<ObjectUrl>) -> AnchoredElement<T> {
        AnchoredElement { inner, anchor }
    }

    /// Returns a reference to the inner storage format element.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Returns a reference to the anchor URL of the artifact.
    pub fn anchor(&self) -> &Arc<ObjectUrl> {
        &self.anchor
    }
}

impl<T> std::ops::Deref for AnchoredElement<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}
