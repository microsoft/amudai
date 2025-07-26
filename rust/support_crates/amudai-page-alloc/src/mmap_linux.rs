use std::sync::OnceLock;

/// Allocates memory using large pages (huge pages) via mmap with the HUGETLB flag.
///
/// This function allocates `size` bytes of memory using large memory pages, which are
/// typically 2MB on most Linux systems. Large pages can improve performance by reducing
/// TLB (Translation Lookaside Buffer) misses for applications that work with large amounts
/// of memory.
///
/// # Arguments
///
/// * `size` - The number of bytes to allocate. The actual allocation will be rounded up
///   to the nearest large page boundary.
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok((ptr, capacity))` - A tuple with a pointer to the allocated memory and the actual
///   capacity in bytes (which may be larger than the requested size due to page alignment)
/// - `Err(io::Error)` - An I/O error if the allocation fails
///
/// # Prerequisites
///
/// For this function to succeed, huge pages must be enabled and properly configured on the
/// system. This typically requires administrator privileges to configure.
///
/// ## Configuration Examples
///
/// 1. **Enable overcommit huge pages** (allows best-effort allocation):
///    ```bash
///    echo COUNT > /proc/sys/vm/nr_overcommit_hugepages
///    ```
///    where `COUNT` specifies the maximum number of huge pages to allocate on demand.
///
/// 2. **Reserve huge pages at boot time**:
///    ```bash
///    echo COUNT > /proc/sys/vm/nr_hugepages
///    ```
///
/// 3. **Check current huge page configuration**:
///    ```bash
///    cat /proc/meminfo | grep -i huge
///    ```
///
/// # Safety
///
/// The returned pointer must be deallocated using [`free_large_pages`] with the same
/// capacity value to avoid memory leaks. The allocated memory is readable and writable.
///
/// # References
///
/// For detailed information about huge page support and configuration options, see:
/// <https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt>
pub fn allocate_large_pages(size: usize) -> std::io::Result<(*mut std::ffi::c_void, usize)> {
    let page_size = get_large_page_size();
    assert!(page_size.is_power_of_two());
    let capacity = (size.max(1) + page_size - 1) & !(page_size - 1);
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            capacity,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB,
            -1,
            0,
        )
    };
    if ptr.is_null() || ptr == libc::MAP_FAILED {
        let err = std::io::Error::last_os_error();
        return Err(err);
    }
    Ok((ptr, capacity))
}

/// Frees memory that was allocated using large pages.
///
/// This function deallocates memory that was previously allocated with
/// [`allocate_large_pages`]. It must be called with the exact same `size`
/// parameter that was **returned** by the allocation function to ensure proper
/// cleanup.
///
/// # Arguments
///
/// * `ptr` - A pointer to the memory region to deallocate, as returned by
///   [`allocate_large_pages`]
/// * `size` - The capacity in bytes that was returned by [`allocate_large_pages`]
///
/// # Returns
///
/// Returns `Ok(())` on successful deallocation, or an `Err(io::Error)` if
/// the deallocation fails.
///
/// # Safety
///
/// This function is safe to call as long as:
/// - `ptr` was returned by a previous call to [`allocate_large_pages`]
/// - `size` matches the capacity returned by that allocation call
/// - The memory has not already been freed
/// - No other references to the memory exist
pub unsafe fn free_large_pages(ptr: *mut std::ffi::c_void, size: usize) -> std::io::Result<()> {
    unsafe { free(ptr, size) }
}

/// Attempts to enable large page support for the current process.
///
/// This function is currently a no-op on Linux and always returns `Ok(())`, as proper
/// configuration of huge pages must be done administratively at the system level.
///
/// # Returns
///
/// Always returns `Ok(())` in the current implementation.
///
/// # Note
///
/// For detailed information about huge page support and configuration options, see:
/// <https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt>
pub fn try_enable_large_pages() -> std::io::Result<()> {
    Ok(())
}

/// Allocates memory using standard pages via mmap.
///
/// This function allocates `size` bytes of memory using the standard system page size,
/// which is typically 4KB on most systems. The allocation is page-aligned and uses
/// anonymous memory mapping.
///
/// # Arguments
///
/// * `size` - The number of bytes to allocate. The actual allocation will be rounded up
///   to the nearest page boundary.
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok((ptr, capacity))` - A tuple with a pointer to the allocated memory and the actual
///   capacity in bytes (which may be larger than the requested size due to page alignment)
/// - `Err(io::Error)` - An I/O error if the allocation fails
///
/// # Safety
///
/// The returned pointer must be deallocated using [`free`] with the same capacity value
/// to avoid memory leaks. The allocated memory is readable and writable.
pub fn allocate(size: usize) -> std::io::Result<(*mut std::ffi::c_void, usize)> {
    let page_size = get_page_size();
    assert!(page_size.is_power_of_two());
    let capacity = (size.max(1) + page_size - 1) & !(page_size - 1);
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            capacity,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if ptr.is_null() || ptr == libc::MAP_FAILED {
        let err = std::io::Error::last_os_error();
        return Err(err);
    }
    Ok((ptr, capacity))
}

/// Frees memory that was allocated using standard pages.
///
/// This function deallocates memory that was previously allocated with [`allocate`].
/// It must be called with the exact same `size` parameter that was **returned** by the
/// allocation function to ensure proper cleanup.
///
/// # Arguments
///
/// * `ptr` - A pointer to the memory region to deallocate, as returned by [`allocate`]
/// * `size` - The capacity in bytes that was returned by [`allocate`]
///
/// # Returns
///
/// Returns `Ok(())` on successful deallocation, or an `Err(io::Error)` if
/// the deallocation fails.
///
/// # Safety
///
/// This function is safe to call as long as:
/// - `ptr` was returned by a previous call to [`allocate`]
/// - `size` matches the capacity returned by the allocation call
/// - The memory has not already been freed
/// - No other references to the memory exist
pub unsafe fn free(ptr: *mut std::ffi::c_void, size: usize) -> std::io::Result<()> {
    let res = unsafe { libc::munmap(ptr, size) };
    if res < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Gets the system's large page (huge page) size in bytes.
///
/// This function returns the size of large pages (also known as huge pages) on the current
/// system. The value is cached after the first call for performance. On most Linux systems,
/// this is typically 2MB (2,097,152 bytes).
///
/// # Returns
///
/// The large page size in bytes. If the system's large page size cannot be determined,
/// returns a default value of 2MB (2,097,152 bytes).
///
/// # Implementation Notes
///
/// The function uses lazy initialization with [`OnceLock`] to cache the result of reading
/// the large page size from `/proc/meminfo`. This ensures the system call is only made
/// once, even in multi-threaded environments.
pub fn get_large_page_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    if let Some(&size) = SIZE.get() {
        size
    } else {
        match read_large_page_size() {
            Ok(size) => {
                let _ = SIZE.set(size);
                size
            }
            Err(_) => 2 * 1024 * 1024,
        }
    }
}

/// Gets the system's standard page size in bytes.
///
/// This function returns the size of standard memory pages on the current system.
/// The value is cached after the first call for performance. On most systems, this
/// is typically 4KB (4,096 bytes).
///
/// # Returns
///
/// The page size in bytes. If the system's page size cannot be determined,
/// returns a default value of 4KB (4,096 bytes).
///
/// # Implementation Notes
///
/// The function uses lazy initialization with [`OnceLock`] to cache the result of
/// calling `sysconf(_SC_PAGESIZE)`.
pub fn get_page_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    if let Some(&size) = SIZE.get() {
        size
    } else {
        match read_page_size() {
            Ok(size) => {
                let _ = SIZE.set(size);
                size
            }
            Err(_) => 4 * 1024,
        }
    }
}

/// Reads the large page (huge page) size from the system.
///
/// This function parses `/proc/meminfo` to determine the system's configured large page size.
/// It looks for the "Hugepagesize:" entry and converts the value from kilobytes to bytes.
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(size)` - The large page size in bytes
/// - `Err(io::Error)` - If `/proc/meminfo` cannot be read or parsed
///
/// # Implementation Details
///
/// The function expects the "Hugepagesize:" line in `/proc/meminfo` to be in the format:
/// ```text
/// Hugepagesize:    2048 kB
/// ```
///
/// It extracts the numeric value (2048 in this example) and multiplies by 1024 to convert
/// from kilobytes to bytes.
///
/// # Errors
///
/// This function will return an error if:
/// - `/proc/meminfo` cannot be read (e.g., on non-Linux systems)
/// - The "Hugepagesize:" line is not found
/// - The line format is unexpected or cannot be parsed
fn read_large_page_size() -> std::io::Result<usize> {
    use std::fs;

    let meminfo = fs::read_to_string("/proc/meminfo")?;
    for line in meminfo.lines() {
        if line.starts_with("Hugepagesize:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(size_kb) = parts[1].parse::<usize>() {
                    return Ok(size_kb * 1024);
                }
            }
            break;
        }
    }
    Err(std::io::Error::other("Failed to read Hugepagesize"))
}

/// Reads the standard page size from the system using sysconf.
///
/// This function calls the POSIX `sysconf(_SC_PAGESIZE)` system call to determine
/// the system's standard page size.
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(size)` - The page size in bytes
/// - `Err(io::Error)` - If the system call fails
fn read_page_size() -> std::io::Result<usize> {
    let res = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if res < 0 {
        return Err(std::io::Error::last_os_error());
    }
    assert!(res < i32::MAX as _);
    Ok(res as usize)
}
