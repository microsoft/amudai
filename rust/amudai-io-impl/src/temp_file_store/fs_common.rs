use std::path::Path;

use amudai_budget_tracker::{Allocation, Budget};
use amudai_io::StorageProfile;

pub struct TempContainer {
    budget: Budget,
    container: tempfile::TempDir,
}

impl TempContainer {
    pub fn new(budget: Budget, container: tempfile::TempDir) -> TempContainer {
        TempContainer { budget, container }
    }

    pub fn with_capacity(
        capacity: u64,
        parent_path: Option<&Path>,
    ) -> std::io::Result<TempContainer> {
        let container = if let Some(parent) = parent_path {
            tempfile::tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        };
        let budget = Budget::new(capacity);
        Ok(TempContainer::new(budget, container))
    }

    pub fn budget(&self) -> &Budget {
        &self.budget
    }

    pub fn path(&self) -> &Path {
        self.container.path()
    }

    pub fn allocate(&self, size_hint: Option<u64>) -> std::io::Result<FileAllocation> {
        let size_hint = size_hint.unwrap_or(0);
        let mut allocation = self.budget.allocate(0)?;
        allocation.reserve(size_hint)?;
        if size_hint >= 1024 * 1024 {
            allocation.set_reservation_slice(128 * 1024);
        }
        Ok(FileAllocation::new(allocation))
    }

    pub fn storage_profile(&self) -> StorageProfile {
        Default::default()
    }
}

pub struct FileAllocation(Allocation);

impl FileAllocation {
    pub fn new(allocation: Allocation) -> FileAllocation {
        assert_eq!(allocation.amount(), 0);
        FileAllocation(allocation)
    }

    pub fn size(&self) -> u64 {
        self.0.amount()
    }

    pub fn ensure_at_least(&mut self, expected_size: u64) -> std::io::Result<()> {
        let amount = self.0.amount();
        if amount < expected_size {
            self.0.grow(expected_size - amount)?;
        }
        Ok(())
    }

    pub fn truncate(&mut self, size: u64) {
        let amount = self.0.amount();
        if amount > size {
            self.0.shrink_to(size);
        }
    }
}
