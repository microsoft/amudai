use std::sync::OnceLock;
use windows_sys::Win32::{
    Foundation::{CloseHandle, ERROR_SUCCESS, GetLastError, HANDLE, LUID},
    Security::{
        AdjustTokenPrivileges, LookupPrivilegeValueW, SE_LOCK_MEMORY_NAME, TOKEN_ADJUST_PRIVILEGES,
        TOKEN_PRIVILEGES, TOKEN_QUERY,
    },
    System::{
        Memory::{
            GetLargePageMinimum, MEM_COMMIT, MEM_LARGE_PAGES, MEM_RELEASE, MEM_RESERVE,
            PAGE_READWRITE, VirtualAlloc, VirtualFree,
        },
        SystemInformation::{GetSystemInfo, SYSTEM_INFO},
        Threading::{GetCurrentProcess, OpenProcessToken},
    },
};

/// Allocates memory using large pages.
///
/// This function allocates `size` bytes of memory using large memory pages, which are
/// typically 2MB on Windows. Large pages can improve performance by reducing
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
/// For this function to succeed, large pages must be enabled and properly configured on the
/// system. This typically requires administrator privileges to configure.
/// This can be done e.g. through `secpol.msc` by navigating to
/// `Local Policies`/`User Rights Assignment`/`"Lock pages in memory" policy` and adding the
/// relevant user accounts.
///
/// # Safety
///
/// The returned pointer must be deallocated using [`free_large_pages`] with the same
/// capacity value to avoid memory leaks. The allocated memory is readable and writable.
///
/// # References
///
/// <https://learn.microsoft.com/en-us/windows/win32/memory/large-page-support>
pub fn allocate_large_pages(size: usize) -> std::io::Result<(*mut std::ffi::c_void, usize)> {
    let page_size = get_large_page_size();
    assert!(page_size.is_power_of_two());
    let capacity = (size.max(1) + page_size - 1) & !(page_size - 1);

    unsafe {
        let ptr = VirtualAlloc(
            std::ptr::null_mut(),
            capacity,
            MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES,
            PAGE_READWRITE,
        );

        if ptr.is_null() {
            let error = GetLastError();
            return Err(std::io::Error::from_raw_os_error(error as i32));
        }

        Ok((ptr, capacity))
    }
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
    assert!(size.is_multiple_of(get_large_page_size()));
    unsafe {
        let result = VirtualFree(ptr, 0, MEM_RELEASE);
        if result == 0 {
            let error = GetLastError();
            return Err(std::io::Error::from_raw_os_error(error as i32));
        }
    }
    Ok(())
}

/// Attempts to enable large page support for the current process.
///
/// This function enables the `SeLockMemoryPrivilege` for the current process, which is
/// required to allocate large pages on Windows. The privilege allows the process to
/// lock pages in memory, preventing them from being paged to disk.
///
/// # Returns
///
/// Returns `Ok(())` if the privilege was successfully enabled, or an `Err(io::Error)`
/// if the operation fails. Common failure reasons include:
/// - Insufficient permissions (typically requires administrator privileges)
/// - The privilege is not available on the system
/// - System policy restrictions
///
/// # Note
///
/// This function must be called before attempting to allocate large pages with
/// [`allocate_large_pages`]. The privilege setting persists for the lifetime of
/// the process.
pub fn try_enable_large_pages() -> std::io::Result<()> {
    adjust_lock_memory_privilege()
}

/// Allocates memory using standard pages via `VirtualAlloc`.
///
/// This function allocates `size` bytes of memory using the standard system page size,
/// which is typically 4KB on most systems.
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

    unsafe {
        let ptr = VirtualAlloc(
            std::ptr::null_mut(),
            capacity,
            MEM_COMMIT | MEM_RESERVE,
            PAGE_READWRITE,
        );

        if ptr.is_null() {
            let error = GetLastError();
            return Err(std::io::Error::from_raw_os_error(error as i32));
        }

        Ok((ptr, capacity))
    }
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
    assert!(size.is_multiple_of(get_page_size()));
    unsafe {
        let result = VirtualFree(ptr, 0, MEM_RELEASE);
        if result == 0 {
            let error = GetLastError();
            return Err(std::io::Error::from_raw_os_error(error as i32));
        }
    }
    Ok(())
}

/// Gets the system's large page size in bytes.
///
/// This function returns the size of large pages on the current system. The value
/// is cached after the first call for performance. On most systems, this is typically
/// 2MB (2,097,152 bytes).
///
/// # Returns
///
/// The large page size in bytes. If the system's large page size cannot be determined,
/// returns a default value of 2MB (2,097,152 bytes).
///
/// # Implementation Notes
///
/// The function uses lazy initialization with [`OnceLock`] to cache the result of querying
/// the large page size. This ensures the system call is only made once, even in multi-threaded
/// environments.
pub fn get_large_page_size() -> usize {
    static LARGE_PAGE_SIZE: OnceLock<usize> = OnceLock::new();

    *LARGE_PAGE_SIZE.get_or_init(|| {
        unsafe {
            let large_page_size = GetLargePageMinimum();
            if large_page_size > 0 {
                large_page_size
            } else {
                // Default to 2MB if we can't determine the large page size
                2 * 1024 * 1024
            }
        }
    })
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
/// querying the page size.
pub fn get_page_size() -> usize {
    static PAGE_SIZE: OnceLock<usize> = OnceLock::new();

    *PAGE_SIZE.get_or_init(|| unsafe {
        let mut system_info: SYSTEM_INFO = std::mem::zeroed();
        GetSystemInfo(&mut system_info);
        system_info.dwPageSize as usize
    })
}

/// Enables the `SeLockMemoryPrivilege` for the current process, which is
/// required to allocate large pages on Windows. The privilege allows the process to
/// lock pages in memory.
///
/// # Returns
///
/// Returns `Ok(())` if the privilege was successfully enabled, or an `Err(io::Error)`
/// if the operation fails.
///
/// # Note
///
/// The privilege setting persists for the lifetime of the process.
fn adjust_lock_memory_privilege() -> std::io::Result<()> {
    unsafe {
        let mut token_handle: HANDLE = std::ptr::null_mut();
        let current_process = GetCurrentProcess();

        // Open the process token with the required access rights
        let result = OpenProcessToken(
            current_process,
            TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY,
            &mut token_handle,
        );

        if result == 0 {
            let error = GetLastError();
            return Err(std::io::Error::from_raw_os_error(error as i32));
        }

        // Look up the privilege value for SeLockMemoryPrivilege
        let mut luid = LUID {
            LowPart: 0,
            HighPart: 0,
        };

        let result = LookupPrivilegeValueW(std::ptr::null(), SE_LOCK_MEMORY_NAME, &mut luid);

        if result == 0 {
            CloseHandle(token_handle);
            let error = GetLastError();
            return Err(std::io::Error::from_raw_os_error(error as i32));
        }

        // Set up the TOKEN_PRIVILEGES structure
        let token_privileges = TOKEN_PRIVILEGES {
            PrivilegeCount: 1,
            Privileges: [windows_sys::Win32::Security::LUID_AND_ATTRIBUTES {
                Luid: luid,
                Attributes: windows_sys::Win32::Security::SE_PRIVILEGE_ENABLED,
            }],
        };

        // Adjust the token privileges
        let result = AdjustTokenPrivileges(
            token_handle,
            0, // Do not disable all privileges
            &token_privileges,
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );

        CloseHandle(token_handle);

        if result == 0 {
            let error = GetLastError();
            return Err(std::io::Error::from_raw_os_error(error as i32));
        }

        // It's important to check GetLastError() here to ensure the privilege
        // was actually enabled. A return value of ERROR_SUCCESS from
        // AdjustTokenPrivileges does not guarantee success.
        let last_error = GetLastError();
        if last_error != ERROR_SUCCESS {
            return Err(std::io::Error::from_raw_os_error(last_error as i32));
        }

        Ok(())
    }
}
