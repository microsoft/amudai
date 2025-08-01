use std::sync::{Arc, LazyLock};

use amudai_common::error::Error;

use crate::{
    ReferenceResolver,
    url::{ObjectUrl, RelativePath},
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
            format!(
                "container-scoped reference resolver: invalid reference {} (base: {})",
                reference,
                base.as_str()
            ),
        ))
    }
}

pub fn get() -> Arc<dyn ReferenceResolver> {
    static RESOLVER: LazyLock<Arc<dyn ReferenceResolver>> =
        LazyLock::new(|| Arc::new(ContainerScopedReferenceResolver));
    Arc::clone(&RESOLVER)
}
