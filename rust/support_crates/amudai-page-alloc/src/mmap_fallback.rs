use std::alloc::{Layout, alloc_zeroed, dealloc};

/// Allocates memory using large pages (emulated).
pub fn allocate_large_pages(size: usize) -> std::io::Result<(*mut std::ffi::c_void, usize)> {
    let page_size = get_large_page_size();
    assert!(page_size.is_power_of_two());
    let capacity = (size.max(1) + page_size - 1) & !(page_size - 1);

    // Create a layout with the required alignment (use page_size as alignment)
    let layout = Layout::from_size_align(capacity, page_size)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid layout"))?;

    let ptr = unsafe { alloc_zeroed(layout) };
    if ptr.is_null() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "Failed to allocate memory",
        ));
    }

    Ok((ptr as *mut std::ffi::c_void, capacity))
}

/// Frees memory that was allocated using large pages.
pub unsafe fn free_large_pages(ptr: *mut std::ffi::c_void, size: usize) -> std::io::Result<()> {
    let page_size = get_large_page_size();
    assert!(size.is_multiple_of(page_size));

    let layout = Layout::from_size_align(size, page_size)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid layout"))?;

    unsafe {
        dealloc(ptr as *mut u8, layout);
    }
    Ok(())
}

pub fn try_enable_large_pages() -> std::io::Result<()> {
    Ok(())
}

/// Allocates memory using standard pages (emulated).
pub fn allocate(size: usize) -> std::io::Result<(*mut std::ffi::c_void, usize)> {
    let page_size = get_page_size();
    assert!(page_size.is_power_of_two());
    let capacity = (size.max(1) + page_size - 1) & !(page_size - 1);

    // Create a layout with the required alignment (use page_size as alignment)
    let layout = Layout::from_size_align(capacity, page_size)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid layout"))?;

    let ptr = unsafe { alloc_zeroed(layout) };
    if ptr.is_null() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "Failed to allocate memory",
        ));
    }

    Ok((ptr as *mut std::ffi::c_void, capacity))
}

/// Frees memory that was allocated using standard pages.
pub unsafe fn free(ptr: *mut std::ffi::c_void, size: usize) -> std::io::Result<()> {
    let page_size = get_page_size();
    assert!(size.is_multiple_of(page_size));

    let layout = Layout::from_size_align(size, page_size)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid layout"))?;

    unsafe {
        dealloc(ptr as *mut u8, layout);
    }
    Ok(())
}

/// Returns the "large page" size in bytes.
pub fn get_large_page_size() -> usize {
    2 * 1024 * 1024
}

/// Returns the "standard page" size in bytes.
pub fn get_page_size() -> usize {
    4 * 1024
}
