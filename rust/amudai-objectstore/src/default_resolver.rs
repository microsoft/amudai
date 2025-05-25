use std::sync::{Arc, LazyLock};

use amudai_common::error::Error;

use crate::{
    url::{ObjectUrl, RelativePath},
    ReferenceResolver,
};

pub struct ContainerScopedReferenceResolver;

impl ReferenceResolver for ContainerScopedReferenceResolver {
    fn validate(&self, base: &ObjectUrl, reference: &str) -> amudai_common::Result<()> {
        if reference.is_empty() || RelativePath::is_valid(reference) {
            return Ok(());
        } else if let Ok(reference) = ObjectUrl::parse(reference) {
            if base.has_in_scope(&reference) {
                return Ok(());
            }
        }
        Err(Error::invalid_arg(
            "reference",
            "container-scoped reference resolver: invalid reference",
        ))
    }
}

pub fn get() -> Arc<dyn ReferenceResolver> {
    static RESOLVER: LazyLock<Arc<dyn ReferenceResolver>> =
        LazyLock::new(|| Arc::new(ContainerScopedReferenceResolver));
    Arc::clone(&RESOLVER)
}
