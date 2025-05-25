use std::sync::atomic::{AtomicU64, Ordering};

/// A thread-safe counter that enables multiple consumers to withdraw (if possible) and deposit
/// specific amounts, ensuring that the counter value remains non-negative.
///
/// The `Counter` uses atomic operations to ensure thread-safety and correctness when multiple
/// threads are accessing and modifying the counter concurrently.
pub struct Counter(AtomicU64);

impl Counter {
    /// Creates a new `Counter` with the given initial amount.
    ///
    /// # Arguments
    ///
    /// * `amount` - The initial value of the counter.
    pub fn new(amount: u64) -> Counter {
        Counter(AtomicU64::new(amount))
    }

    /// Attempts to withdraw the specified `amount` from the counter.
    ///
    /// If the current value of the counter is greater than or equal to the `amount`, the `amount`
    /// is subtracted from the counter, and `true` is returned. Otherwise, the counter remains
    /// unchanged, and `false` is returned.
    ///
    /// This operation uses atomic compare-and-exchange operations to ensure correctness when
    /// multiple threads are attempting to withdraw concurrently.
    ///
    /// # Arguments
    ///
    /// * `amount` - The amount to withdraw from the counter.
    ///
    /// # Returns
    ///
    /// `true` if the withdrawal was successful, `false` otherwise.
    pub fn withdraw(&self, amount: u64) -> bool {
        let mut current = self.0.load(Ordering::Relaxed);
        while current >= amount {
            match self.0.compare_exchange_weak(
                current,
                current - amount,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(updated) => current = updated,
            }
        }
        false
    }

    /// Attempts to withdraw any amount from the counter that is above the specified
    /// `threshold`.
    ///
    /// If the current value of the counter is greater than `threshold`, the difference is
    /// withdrawn, and the withdrawn amount is returned. Otherwise, the counter remains
    /// unchanged, and `0` is returned.
    ///
    /// This operation uses atomic compare-and-exchange operations to ensure correctness when
    /// multiple threads are attempting to withdraw concurrently.
    ///
    /// # Arguments
    ///
    /// * `threshold` - The minimum amount to keep in the counter.
    ///
    /// # Returns
    ///
    /// The amount withdrawn from the counter, or `0` if no withdrawal was made.
    pub fn withdraw_above(&self, threshold: u64) -> u64 {
        let mut current = self.0.load(Ordering::Relaxed);
        while current > threshold {
            match self.0.compare_exchange_weak(
                current,
                threshold,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return current - threshold,
                Err(updated) => current = updated,
            }
        }
        0
    }

    /// Deposits the specified `amount` into the counter.
    ///
    /// The `amount` is added to the counter.
    ///
    /// This operation uses atomic fetch-and-add operations to ensure correctness when
    /// multiple threads are depositing concurrently.
    ///
    /// # Arguments
    ///
    /// * `amount` - The amount to deposit into the counter.
    pub fn deposit(&self, amount: u64) {
        self.0.fetch_add(amount, Ordering::Release);
    }

    /// Returns the counter value (most likely stale by the time it is observed by the caller).
    pub fn read(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Drains the counter, setting its value to 0 and returning the previous
    /// remaining amount.
    ///
    /// # Returns
    ///
    /// The previous value of the counter before it was drained.
    pub fn drain(&mut self) -> u64 {
        std::mem::replace(self.0.get_mut(), 0)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;

    #[test]
    fn test_new() {
        let counter = Counter::new(100);
        assert_eq!(counter.0.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_try_withdraw_success() {
        let counter = Counter::new(100);
        assert!(counter.withdraw(50));
        assert_eq!(counter.0.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_try_withdraw_fail() {
        let counter = Counter::new(100);
        assert!(!counter.withdraw(150));
        assert_eq!(counter.0.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_try_withdraw_zero() {
        let counter = Counter::new(100);
        assert!(counter.withdraw(0));
        assert_eq!(counter.0.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_try_withdraw_exact() {
        let counter = Counter::new(100);
        assert!(counter.withdraw(100));
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_try_withdraw_multiple() {
        let counter = Counter::new(100);
        assert!(counter.withdraw(30));
        assert!(counter.withdraw(20));
        assert_eq!(counter.0.load(Ordering::Relaxed), 50);
        assert!(!counter.withdraw(60));
        assert_eq!(counter.0.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_try_withdraw_above_success() {
        let counter = Counter::new(100);
        assert_eq!(counter.withdraw_above(50), 50);
        assert_eq!(counter.0.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_try_withdraw_above_fail() {
        let counter = Counter::new(50);
        assert_eq!(counter.withdraw_above(100), 0);
        assert_eq!(counter.0.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_try_withdraw_above_zero() {
        let counter = Counter::new(100);
        assert_eq!(counter.withdraw_above(0), 100);
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_try_withdraw_above_exact() {
        let counter = Counter::new(100);
        assert_eq!(counter.withdraw_above(100), 0);
        assert_eq!(counter.0.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_try_withdraw_above_multiple() {
        let counter = Counter::new(150);
        assert_eq!(counter.withdraw_above(100), 50);
        assert_eq!(counter.withdraw_above(50), 50);
        assert_eq!(counter.0.load(Ordering::Relaxed), 50);
        assert_eq!(counter.withdraw_above(60), 0);
        assert_eq!(counter.0.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_deposit() {
        let counter = Counter::new(100);
        counter.deposit(50);
        assert_eq!(counter.0.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_deposit_zero() {
        let counter = Counter::new(100);
        counter.deposit(0);
        assert_eq!(counter.0.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_deposit_multiple() {
        let counter = Counter::new(100);
        counter.deposit(50);
        counter.deposit(25);
        assert_eq!(counter.0.load(Ordering::Relaxed), 175);
    }

    #[test]
    fn test_drain() {
        let mut counter = Counter::new(100);
        let drained_value = counter.drain();
        assert_eq!(drained_value, 100);
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_drain_zero() {
        let mut counter = Counter::new(0);
        let drained_value = counter.drain();
        assert_eq!(drained_value, 0);
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_concurrent_withdraw() {
        let counter = Arc::new(Counter::new(1000));
        let num_threads = 10;
        let withdraw_amount = 100;
        let mut handles = vec![];

        for _ in 0..num_threads {
            let counter_ref = counter.clone();
            handles.push(std::thread::spawn(move || {
                let mut withdrawn = 0;
                for _ in 0..10 {
                    if counter_ref.withdraw(withdraw_amount) {
                        withdrawn += withdraw_amount;
                    }
                    std::thread::sleep(Duration::from_millis(1)); // Introduce some contention
                }
                withdrawn
            }));
        }

        let total_withdrawn: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total_withdrawn, 1000);
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_concurrent_withdraw_above() {
        let counter = Arc::new(Counter::new(1000));
        let num_threads = 10;
        let threshold = 0;
        let mut handles = vec![];

        for _ in 0..num_threads {
            let counter_ref = counter.clone();
            handles.push(std::thread::spawn(move || {
                let mut withdrawn = 0;
                for _ in 0..10 {
                    withdrawn += counter_ref.withdraw_above(threshold);
                    std::thread::sleep(Duration::from_millis(1)); // Introduce some contention
                }
                withdrawn
            }));
        }

        let total_withdrawn: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total_withdrawn, 1000);
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_concurrent_deposit() {
        let counter = Arc::new(Counter::new(0));
        let num_threads = 10;
        let deposit_amount = 100;
        let mut handles = vec![];

        for _ in 0..num_threads {
            let counter_ref = counter.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..10 {
                    counter_ref.deposit(deposit_amount);
                    std::thread::sleep(Duration::from_millis(1)); // Introduce some contention
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(counter.0.load(Ordering::Relaxed), 10000);
    }

    #[test]
    fn test_concurrent_deposit_and_withdraw() {
        let counter = Arc::new(Counter::new(500));
        let num_threads = 10;
        let deposit_amount = 100;
        let withdraw_amount = 50;
        let mut handles = vec![];

        for i in 0..num_threads {
            let counter_ref = counter.clone();
            handles.push(std::thread::spawn(move || {
                if i % 2 == 0 {
                    for _ in 0..10 {
                        counter_ref.deposit(deposit_amount);
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    0
                } else {
                    let mut withdrawn = 0;
                    for _ in 0..10 {
                        if counter_ref.withdraw(withdraw_amount) {
                            withdrawn += withdraw_amount;
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    withdrawn
                }
            }));
        }

        let mut total_withdrawn = 0;
        for h in handles {
            if let Ok(withdrawn) = h.join() {
                total_withdrawn += withdrawn;
            }
        }

        let expected_final_value =
            500 + (num_threads / 2) as u64 * 10 * deposit_amount - total_withdrawn;
        assert_eq!(counter.0.load(Ordering::Relaxed), expected_final_value);
    }
}
