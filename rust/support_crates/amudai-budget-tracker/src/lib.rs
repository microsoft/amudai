use std::sync::Arc;

use counter::Counter;

pub mod counter;

/// Represents a budget that can be allocated from.
#[derive(Clone)]
pub struct Budget(Arc<BudgetNode>);

impl Budget {
    /// Creates a new root budget with the given amount.
    pub fn new(amount: u64) -> Budget {
        Budget(BudgetNode::new_root(amount))
    }

    /// Returns the remaining amount in this budget tracker.
    ///
    /// **Note**: This method is primarily intended for diagnostic purposes. The returned
    /// value may be outdated in a concurrent environment.
    pub fn remaining(&self) -> u64 {
        self.0.remaining()
    }

    /// Attempts to allocate the specified amount from the budget.
    ///
    /// Returns an `Allocation` upon success, or an `AllocationError` if the budget
    /// lacks sufficient remaining funds.
    ///
    /// The allocated amount is automatically returned to the `Budget` when the
    /// `Allocation` is dropped.
    pub fn allocate(&self, amount: u64) -> Result<Allocation, AllocationError> {
        if self.0.allocate(amount) {
            Ok(Allocation {
                budget: self.0.clone(),
                amount,
                reservation: 0,
                reservation_slice: 0,
            })
        } else {
            Err(AllocationError)
        }
    }

    /// Creates a separate budget tracker with the specified amount, borrowed from the
    /// current budget.
    ///
    /// The borrowed budget is returned to the parent once it is no longer needed.
    ///
    /// `split_off` is a specific instance of a more general "subordinate" budget.
    pub fn split_off(&self, amount: u64) -> Result<Budget, AllocationError> {
        self.subordinate()
            .budget(amount)
            .reserve(amount)
            .reservation_slice(0)
            .create()
    }

    /// Constructs a subordinate budget tracker.
    ///
    /// The subordinate budget has its own limit on the total allocated amount.
    /// Additionally, every allocation from the subordinate budget is also tracked
    /// by the parent budget. For an allocation to succeed, both the parent and
    /// subordinate budgets must have sufficient available funds.
    pub fn subordinate(&self) -> SubordinateBuilder {
        SubordinateBuilder {
            node: self.0.clone(),
            budget: None,
            reservation: None,
            reservation_slice: None,
        }
    }
}

#[derive(Clone)]
pub struct SubordinateBuilder {
    node: Arc<BudgetNode>,
    budget: Option<u64>,
    reservation: Option<u64>,
    reservation_slice: Option<u64>,
}

impl SubordinateBuilder {
    /// Configures the budget limit for the subordinate budget tracker.
    pub fn budget(mut self, budget: u64) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Sets the initial reservation for the subordinate budget.
    ///
    /// This amount is reserved from the parent budget when the subordinate budget
    /// is created. The reservation ensures that consumers can allocate at least
    /// this amount from the subordinate tracker.
    pub fn reserve(mut self, reservation: u64) -> Self {
        self.reservation = Some(reservation);
        self
    }

    pub fn reservation_slice(mut self, slice: u64) -> Self {
        self.reservation_slice = Some(slice);
        self
    }

    /// Creates a subordinate budget.
    ///
    /// Returns an `AllocationError` if the parent budget lacks sufficient
    /// remaining funds to accommodate the reservation.
    pub fn create(self) -> Result<Budget, AllocationError> {
        let node = self
            .node
            .create_child(
                self.budget.unwrap_or(0),
                self.reservation.unwrap_or(0),
                self.reservation_slice.unwrap_or(0),
            )
            .ok_or(AllocationError)?;
        Ok(Budget(node))
    }
}

/// Represents an allocation from a budget.
///
/// This structure tracks the amount that has been allocated and allows for its adjustment as needed.
/// When the allocation is dropped, the allocated amount is returned to the parent budget tracker.
pub struct Allocation {
    budget: Arc<BudgetNode>,
    amount: u64,
    reservation: u64,
    reservation_slice: u64,
}

impl Allocation {
    /// Currently allocated amount.
    pub fn amount(&self) -> u64 {
        self.amount
    }

    /// Currently reserved amount (on top of the allocated one).
    ///
    /// Calling `grow(n)` where `n <= reservation` will always succeed.
    pub fn reservation(&self) -> u64 {
        self.reservation
    }

    /// Current total capacity. This is the sum of the allocated amount
    /// and the reservation.
    ///
    /// This is the amount taken from the budget by this allocation.
    pub fn capacity(&self) -> u64 {
        self.amount + self.reservation
    }

    /// Grows the allocation by the given amount.
    pub fn grow(&mut self, additional: u64) -> Result<(), AllocationError> {
        if additional > self.reservation {
            self.reserve(additional.max(self.reservation_slice))?;
        }

        assert!(additional <= self.reservation);
        self.reservation -= additional;
        self.amount += additional;
        Ok(())
    }

    pub fn shrink_to(&mut self, amount: u64) {
        if amount <= self.amount {
            self.budget.release(self.amount - amount + self.reservation);
            self.amount = amount;
            self.reservation = 0;
        }
    }

    /// Reserves the specified amount for subsequent allocations.
    pub fn reserve(&mut self, reservation: u64) -> Result<(), AllocationError> {
        if reservation <= self.reservation {
            return Ok(());
        }
        let to_allocate = reservation - self.reservation;
        if self.budget.allocate(to_allocate) {
            self.reservation = reservation;
            Ok(())
        } else {
            Err(AllocationError)
        }
    }

    pub fn set_reservation_slice(&mut self, reservation_slice: u64) {
        self.reservation_slice = reservation_slice;
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        let to_release = self.capacity();
        if to_release != 0 {
            self.budget.release(to_release);
        }
    }
}

impl std::fmt::Debug for Allocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Allocation")
            .field("amount", &self.amount)
            .field("reservation", &self.reservation)
            .finish_non_exhaustive()
    }
}

/// An error that occurs when a budget allocation fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllocationError;

impl std::fmt::Display for AllocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Budget allocation error")
    }
}

impl std::error::Error for AllocationError {}

impl From<AllocationError> for std::io::Error {
    fn from(e: AllocationError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }
}

/// A node in the budget tree.
struct BudgetNode {
    /// Parent budget node. If present, all allocations from the current node must also
    /// allocate from this parent budget node in order to succeed.
    parent: Option<Arc<BudgetNode>>,
    /// Remaining budget in this node.
    remaining_budget: Counter,
    /// Cached parent allocation. When present, attempt to allocate from this cache
    /// before trying to allocate from the parent directly.
    parent_reservation: Option<Counter>,
    /// Allocation granularity from the parent budget when filling the
    /// `cached_parent_allocation`.
    reservation_slice: u64,
}

impl BudgetNode {
    /// Creates a new root budget node.
    fn new_root(amount: u64) -> Arc<BudgetNode> {
        Arc::new(BudgetNode {
            parent: None,
            remaining_budget: Counter::new(amount),
            parent_reservation: None,
            reservation_slice: 0,
        })
    }

    fn remaining(&self) -> u64 {
        self.remaining_budget.read()
    }

    fn create_child(
        self: &Arc<Self>,
        budget: u64,
        reservation: u64,
        reservation_slice: u64,
    ) -> Option<Arc<BudgetNode>> {
        if reservation != 0 && !self.allocate(reservation) {
            return None;
        }
        Some(Arc::new(BudgetNode {
            parent: Some(self.clone()),
            remaining_budget: Counter::new(budget),
            parent_reservation: (reservation_slice != 0 || reservation != 0)
                .then(|| Counter::new(reservation)),
            reservation_slice,
        }))
    }

    fn allocate(&self, amount: u64) -> bool {
        if amount == 0 {
            return true;
        }

        if !self.remaining_budget.withdraw(amount) {
            return false;
        }

        if !self.allocate_from_parent(amount) {
            self.remaining_budget.deposit(amount);
            return false;
        }

        true
    }

    fn release(&self, amount: u64) {
        if let Some(parent) = self.parent.as_deref() {
            if let Some(parent_reservation) = self.parent_reservation.as_ref() {
                parent_reservation.deposit(amount);
                if self.reservation_slice != 0 {
                    let excess = parent_reservation.withdraw_above(self.reservation_slice);
                    if excess != 0 {
                        parent.release(excess);
                    }
                }
            } else {
                parent.release(amount);
            }
        }
        self.remaining_budget.deposit(amount);
    }

    fn allocate_from_parent(&self, amount: u64) -> bool {
        let Some(parent) = self.parent.as_deref() else {
            return true;
        };

        // Try to allocate from the parent cache first.
        if let Some(parent_reservation) = self.parent_reservation.as_ref() {
            if parent_reservation.withdraw(amount) {
                return true;
            }

            if self.reservation_slice != 0 {
                let parent_allocation = amount.max(self.reservation_slice);
                // If this fails, fall though to allocate the required amount from the parent
                // directly.
                if parent.allocate(parent_allocation) {
                    parent_reservation.deposit(parent_allocation - amount);
                    return true;
                }
            }
        }

        parent.allocate(amount)
    }
}

impl Drop for BudgetNode {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.take() {
            if let Some(mut reservation) = self.parent_reservation.take() {
                parent.release(reservation.drain());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_budget_creation() {
        let budget = Budget::new(100);
        assert_eq!(budget.remaining(), 100);
    }

    #[test]
    fn test_allocate_success() {
        let budget = Budget::new(100);
        let allocation = budget.allocate(50).unwrap();
        assert_eq!(allocation.amount(), 50);
        assert_eq!(budget.remaining(), 50);
    }

    #[test]
    fn test_allocate_failure() {
        let budget = Budget::new(100);
        let result = budget.allocate(150);
        assert_eq!(result.unwrap_err(), AllocationError);
        assert_eq!(budget.remaining(), 100);
    }

    #[test]
    fn test_allocation_drop() {
        let budget = Budget::new(100);
        {
            let _allocation = budget.allocate(50).unwrap();
        }
        assert_eq!(budget.remaining(), 100);
    }

    #[test]
    fn test_allocation_amount() {
        let budget = Budget::new(100);
        let allocation = budget.allocate(50).unwrap();
        assert_eq!(allocation.amount(), 50);
    }

    #[test]
    fn test_allocation_grow_success() {
        let budget = Budget::new(100);
        let mut allocation = budget.allocate(50).unwrap();
        allocation.grow(20).unwrap();
        assert_eq!(allocation.amount(), 70);
        assert_eq!(budget.remaining(), 30);
    }

    #[test]
    fn test_allocation_grow_failure() {
        let budget = Budget::new(100);
        let mut allocation = budget.allocate(50).unwrap();
        let result = allocation.grow(60);
        assert_eq!(result, Err(AllocationError));
        assert_eq!(allocation.amount(), 50);
        assert_eq!(budget.remaining(), 50);
    }

    #[test]
    fn test_allocation_reserve_success() {
        let budget = Budget::new(100);
        let mut allocation = budget.allocate(50).unwrap();
        let res = allocation.reserve(70);
        assert!(res.is_err());
        assert_eq!(allocation.reservation(), 0);
        assert_eq!(budget.remaining(), 50);
    }

    #[test]
    fn test_allocation_reserve_failure() {
        let budget = Budget::new(100);
        let mut allocation = budget.allocate(50).unwrap();
        let result = allocation.reserve(120);
        assert_eq!(result, Err(AllocationError));
        assert_eq!(allocation.reservation(), 0);
        assert_eq!(budget.remaining(), 50);
    }

    #[test]
    fn test_allocation_reserve_no_op() {
        let budget = Budget::new(100);
        let mut allocation = budget.allocate(50).unwrap();
        allocation.reserve(30).unwrap();
        assert_eq!(allocation.reservation(), 30);
        allocation.reserve(20).unwrap();
        assert_eq!(allocation.reservation(), 30);
        assert_eq!(budget.remaining(), 20);
    }

    #[test]
    fn test_allocation_grow_with_reservation() {
        let budget = Budget::new(100);
        let mut allocation = budget.allocate(50).unwrap();
        allocation.reserve(20).unwrap();
        allocation.grow(10).unwrap();
        assert_eq!(allocation.amount(), 60);
        assert_eq!(allocation.reservation(), 10);
        assert_eq!(budget.remaining(), 30);
    }

    #[test]
    fn test_split_off_success() {
        let budget = Budget::new(100);
        let sub_budget = budget.split_off(50).unwrap();
        let allocation = sub_budget.allocate(20).unwrap();
        assert_eq!(allocation.amount(), 20);
        assert_eq!(budget.remaining(), 50);
        assert_eq!(sub_budget.remaining(), 30);
    }

    #[test]
    fn test_split_off_failure() {
        let budget = Budget::new(100);
        let result = budget.split_off(150);
        assert!(result.is_err());
        assert_eq!(budget.remaining(), 100);
    }

    #[test]
    fn test_subordinate_budget_creation_success() {
        let budget = Budget::new(100);
        let sub_budget = budget.subordinate().budget(50).create().unwrap();
        assert_eq!(sub_budget.remaining(), 50);
        assert_eq!(budget.remaining(), 100);
    }

    #[test]
    fn test_subordinate_budget_creation_failure() {
        let budget = Budget::new(100);
        let result = budget.subordinate().budget(50).reserve(150).create();
        assert!(result.is_err());
        assert_eq!(budget.remaining(), 100);
    }

    #[test]
    fn test_subordinate_budget_allocation_success() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .create()
            .unwrap();
        let allocation = sub_budget.allocate(20).unwrap();
        assert_eq!(allocation.amount(), 20);
        assert_eq!(budget.remaining(), 70);
        assert_eq!(sub_budget.remaining(), 30);
    }

    #[test]
    fn test_subordinate_budget_allocation_failure() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .create()
            .unwrap();
        let result = sub_budget.allocate(60);
        assert_eq!(result.unwrap_err(), AllocationError);
        assert_eq!(budget.remaining(), 70);
        assert_eq!(sub_budget.remaining(), 50);
    }

    #[test]
    fn test_subordinate_budget_allocation_parent_failure() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(150)
            .reserve(0)
            .create()
            .unwrap();
        let result = sub_budget.allocate(120);
        assert_eq!(result.unwrap_err(), AllocationError);
        assert_eq!(budget.remaining(), 100);
        assert_eq!(sub_budget.remaining(), 150);
    }

    #[test]
    fn test_subordinate_budget_drop() {
        let budget = Budget::new(100);
        {
            let _sub_budget = budget
                .subordinate()
                .budget(50)
                .reserve(30)
                .create()
                .unwrap();
        }
        assert_eq!(budget.remaining(), 100);
    }

    #[test]
    fn test_subordinate_budget_allocation_with_reservation_slice() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .reservation_slice(10)
            .create()
            .unwrap();
        let allocation = sub_budget.allocate(20).unwrap();
        assert_eq!(allocation.amount(), 20);
        assert_eq!(budget.remaining(), 70);
        assert_eq!(sub_budget.remaining(), 30);
    }

    #[test]
    fn test_subordinate_budget_allocation_with_reservation_slice_grow() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .reservation_slice(10)
            .create()
            .unwrap();
        let mut allocation = sub_budget.allocate(20).unwrap();
        allocation.grow(10).unwrap();
        assert_eq!(allocation.amount(), 30);
        assert_eq!(budget.remaining(), 70);
        assert_eq!(sub_budget.remaining(), 20);
    }

    #[test]
    fn test_subordinate_budget_allocation_with_reservation_slice_grow_above_slice() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .reservation_slice(10)
            .create()
            .unwrap();
        let mut allocation = sub_budget.allocate(20).unwrap();
        allocation.grow(20).unwrap();
        assert_eq!(allocation.amount(), 40);
        assert_eq!(budget.remaining(), 50);
        assert_eq!(sub_budget.remaining(), 10);
    }

    #[test]
    fn test_subordinate_budget_allocation_with_reservation_slice_grow_failure() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .reservation_slice(10)
            .create()
            .unwrap();
        let mut allocation = sub_budget.allocate(20).unwrap();
        let result = allocation.grow(40);
        assert_eq!(result, Err(AllocationError));
        assert_eq!(allocation.amount(), 20);
        assert_eq!(budget.remaining(), 70);
        assert_eq!(sub_budget.remaining(), 30);
    }

    #[test]
    fn test_allocation_set_reservation_slice() {
        let budget = Budget::new(100);
        let mut allocation = budget.allocate(50).unwrap();
        allocation.set_reservation_slice(20);
        allocation.reserve(30).unwrap();
        assert_eq!(allocation.reservation(), 30);
    }

    #[test]
    fn test_subordinate_budget_allocation_with_reservation_slice_and_grow_multiple() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .reservation_slice(10)
            .create()
            .unwrap();
        let mut allocation = sub_budget.allocate(10).unwrap();
        allocation.grow(5).unwrap();
        allocation.grow(5).unwrap();
        allocation.grow(10).unwrap();
        assert_eq!(allocation.amount(), 30);
        assert_eq!(budget.remaining(), 70);
        assert_eq!(sub_budget.remaining(), 20);
    }

    #[test]
    fn test_subordinate_budget_allocation_with_reservation_slice_and_grow_multiple_above_slice() {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .reservation_slice(10)
            .create()
            .unwrap();
        let mut allocation = sub_budget.allocate(10).unwrap();
        allocation.grow(10).unwrap();
        allocation.grow(10).unwrap();
        allocation.grow(10).unwrap();
        assert_eq!(allocation.amount(), 40);
        assert_eq!(budget.remaining(), 60);
        assert_eq!(sub_budget.remaining(), 10);
    }

    #[test]
    fn test_subordinate_budget_allocation_with_reservation_slice_and_grow_multiple_above_slice_and_fail()
     {
        let budget = Budget::new(100);
        let sub_budget = budget
            .subordinate()
            .budget(50)
            .reserve(30)
            .reservation_slice(10)
            .create()
            .unwrap();
        let mut allocation = sub_budget.allocate(10).unwrap();
        allocation.grow(10).unwrap();
        allocation.grow(10).unwrap();
        let result = allocation.grow(30);
        assert_eq!(result, Err(AllocationError));
        assert_eq!(allocation.amount(), 30);
        assert_eq!(budget.remaining(), 70);
        assert_eq!(sub_budget.remaining(), 20);
    }
}
