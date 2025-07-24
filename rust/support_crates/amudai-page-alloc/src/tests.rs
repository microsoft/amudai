use crate::{mmap, mmap_buffer};

#[test]
fn test_normal_allocations() {
    let p = Pages::allocate_normal(1).unwrap();
    assert!(!p.ptr.is_null());
    assert!(p.size >= mmap::get_page_size());
    assert!(p.is_aligned(mmap::get_page_size()));

    let p = Pages::allocate_normal(0).unwrap();
    assert!(!p.ptr.is_null());
    assert!(p.size >= mmap::get_page_size());
    assert!(p.is_aligned(mmap::get_page_size()));
}

#[test]
fn test_allocate_standard_pages_basic() {
    let size = 1024;
    let pages = Pages::allocate_normal(size).expect("allocate_normal 1024");
    assert!(pages.size >= size);
    assert!(pages.is_aligned(mmap::get_page_size()));
}

#[test]
fn test_allocate_standard_pages_zero_size() {
    let pages = Pages::allocate_normal(0).expect("allocate");
    assert_eq!(
        pages.size,
        mmap::get_page_size(),
        "Zero size should allocate one page"
    );
}

#[test]
fn test_allocate_standard_pages_exact_page_size() {
    let page_size = mmap::get_page_size();
    let pages = Pages::allocate_normal(page_size).expect("allocate");
    assert_eq!(pages.size, page_size);
}

#[test]
fn test_allocate_standard_pages_multiple_pages() {
    let page_size = mmap::get_page_size();
    let size = page_size * 3 + 100; // Should round up to 4 pages
    let result = Pages::allocate_normal(size).expect("allocate");
    assert_eq!(result.size, page_size * 4);
}

#[test]
fn test_large_page_allocations() {
    if let Err(e) = mmap_buffer::check_and_enable_large_page_support() {
        println!("check_and_enable_large_page_support: {e:?}");
        return;
    }

    let pages = Pages::allocate_large(50000000).unwrap();
    assert!(!pages.ptr.is_null());
    assert!(pages.is_aligned(mmap::get_large_page_size()));

    let pages = Pages::allocate_large(0).unwrap();
    assert!(!pages.ptr.is_null());
    assert!(pages.is_aligned(mmap::get_large_page_size()));
}

struct Pages {
    ptr: *mut std::ffi::c_void,
    size: usize,
    is_large: bool,
}

impl Pages {
    fn allocate_normal(size: usize) -> std::io::Result<Pages> {
        let (ptr, size) = crate::mmap::allocate(size)?;
        Ok(Pages {
            ptr,
            size,
            is_large: false,
        })
    }

    fn allocate_large(size: usize) -> std::io::Result<Pages> {
        let (ptr, size) = crate::mmap::allocate_large_pages(size)?;
        Ok(Pages {
            ptr,
            size,
            is_large: true,
        })
    }

    fn is_aligned(&self, alignment: usize) -> bool {
        (self.ptr as usize).is_multiple_of(alignment)
    }
}

impl Drop for Pages {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            if !self.is_large {
                unsafe {
                    crate::mmap::free(self.ptr, self.size).expect("free");
                }
            } else {
                unsafe {
                    crate::mmap::free_large_pages(self.ptr, self.size).expect("free_large_pages");
                }
            }
        }
    }
}

// MmapBuffer tests
use crate::mmap_buffer::MmapBuffer;

#[test]
fn test_mmap_buffer_allocate_regular() {
    let size = 1024;
    let buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate regular buffer");

    assert_eq!(buffer.len(), size);
    assert!(buffer.capacity() >= size);
    assert!(buffer.capacity() >= MmapBuffer::regular_page_size());
    assert!(!buffer.uses_large_pages());
    assert_eq!(buffer.alignment(), MmapBuffer::regular_page_size());
    assert!(!buffer.ptr().is_null());
    assert!(!buffer.is_empty());
}

#[test]
fn test_mmap_buffer_allocate_regular_zero_size() {
    let buffer = MmapBuffer::allocate_regular(0).expect("Failed to allocate zero-size buffer");

    assert_eq!(buffer.len(), 0);
    assert!(buffer.capacity() >= MmapBuffer::regular_page_size());
    assert!(!buffer.uses_large_pages());
    assert_eq!(buffer.alignment(), MmapBuffer::regular_page_size());
    assert!(!buffer.ptr().is_null());
    assert!(buffer.is_empty());
}

#[test]
fn test_mmap_buffer_allocate_large_pages() {
    // Skip test if large pages are not available
    if mmap_buffer::check_and_enable_large_page_support().is_err() {
        println!("Large pages not available, skipping test");
        return;
    }

    let size = 1024 * 1024; // 1MB
    if let Ok(buffer) = MmapBuffer::allocate_large_pages(size) {
        assert_eq!(buffer.len(), size);
        assert!(buffer.capacity() >= size);
        assert!(buffer.capacity() >= MmapBuffer::large_page_size());
        assert!(buffer.uses_large_pages());
        assert_eq!(buffer.alignment(), MmapBuffer::large_page_size());
        assert!(!buffer.ptr().is_null());
        assert!(!buffer.is_empty());
    }
}

#[test]
fn test_mmap_buffer_allocate_with_fallback() {
    let size = 1024;
    let buffer =
        MmapBuffer::allocate_with_fallback(size).expect("Failed to allocate with fallback");

    assert_eq!(buffer.len(), size);
    assert!(buffer.capacity() >= size);
    assert!(!buffer.ptr().is_null());
    assert!(!buffer.is_empty());

    // Should either use large pages or regular pages
    if buffer.uses_large_pages() {
        assert_eq!(buffer.alignment(), MmapBuffer::large_page_size());
        assert!(buffer.capacity() >= MmapBuffer::large_page_size());
    } else {
        assert_eq!(buffer.alignment(), MmapBuffer::regular_page_size());
        assert!(buffer.capacity() >= MmapBuffer::regular_page_size());
    }
}

#[test]
fn test_mmap_buffer_resize() {
    let initial_size = 1024;
    let mut buffer = MmapBuffer::allocate_regular(initial_size).expect("Failed to allocate buffer");

    // Test growing within capacity
    let new_size = buffer.capacity() / 2;
    buffer.resize(new_size);
    assert_eq!(buffer.len(), new_size);

    // Test shrinking
    buffer.resize(512);
    assert_eq!(buffer.len(), 512);

    // Test growing beyond capacity (should be clamped)
    let large_size = buffer.capacity() + 1000;
    buffer.resize(large_size);
    assert_eq!(buffer.len(), buffer.capacity());

    // Test resizing to zero
    buffer.resize(0);
    assert_eq!(buffer.len(), 0);
    assert!(buffer.is_empty());
}

#[test]
fn test_mmap_buffer_as_bytes() {
    let size = 1024;
    let buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate buffer");

    let bytes = buffer.as_bytes();
    assert_eq!(bytes.len(), size);

    // Test that all bytes are initially zero
    assert!(bytes.iter().all(|&b| b == 0));
}

#[test]
fn test_mmap_buffer_as_bytes_mut() {
    let size = 1024;
    let mut buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate buffer");

    // Write some data
    {
        let bytes_mut = buffer.as_bytes_mut();
        assert_eq!(bytes_mut.len(), size);
        bytes_mut[0] = 42;
        bytes_mut[100] = 123;
        bytes_mut[size - 1] = 255;
    }

    // Verify the data was written
    let bytes = buffer.as_bytes();
    assert_eq!(bytes[0], 42);
    assert_eq!(bytes[100], 123);
    assert_eq!(bytes[size - 1], 255);
}

#[test]
fn test_mmap_buffer_as_slice() {
    let size = 1024;
    let mut buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate buffer");

    // Write some u32 values
    {
        let slice_mut = buffer.as_mut_slice::<u32>();
        assert_eq!(slice_mut.len(), size / 4);
        slice_mut[0] = 0x12345678;
        slice_mut[1] = 0xABCDEF00;
    }

    // Read back as u32 slice
    let slice = buffer.as_slice::<u32>();
    assert_eq!(slice.len(), size / 4);
    assert_eq!(slice[0], 0x12345678);
    assert_eq!(slice[1], 0xABCDEF00);
}

#[test]
fn test_mmap_buffer_deref() {
    let size = 1024;
    let mut buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate buffer");

    // Test immutable deref
    assert_eq!(buffer.len(), size);
    assert!(buffer.iter().all(|&b| b == 0));

    // Test mutable deref
    buffer[0] = 42;
    buffer[size - 1] = 123;

    assert_eq!(buffer[0], 42);
    assert_eq!(buffer[size - 1], 123);
}

#[test]
fn test_mmap_buffer_as_ref() {
    let size = 1024;
    let buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate buffer");

    let bytes_ref: &[u8] = buffer.as_ref();
    assert_eq!(bytes_ref.len(), size);
    assert!(bytes_ref.iter().all(|&b| b == 0));
}

#[test]
fn test_mmap_buffer_debug() {
    let size = 1024;
    let buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate buffer");

    let debug_str = format!("{:?}", buffer);
    assert!(debug_str.contains("MmapBuffer"));
    assert!(debug_str.contains("ptr"));
    assert!(debug_str.contains("len"));
    assert!(debug_str.contains("capacity"));
}

#[test]
fn test_mmap_buffer_page_sizes() {
    let regular_page_size = MmapBuffer::regular_page_size();
    let large_page_size = MmapBuffer::large_page_size();

    assert!(regular_page_size > 0);
    assert!(regular_page_size.is_power_of_two());

    assert!(large_page_size > 0);
    assert!(large_page_size.is_power_of_two());
    assert!(large_page_size >= regular_page_size);
}

#[test]
fn test_mmap_buffer_alignment() {
    let size = 1024;
    let buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate buffer");

    let ptr_addr = buffer.ptr() as usize;
    let alignment = buffer.alignment();

    assert!(
        ptr_addr % alignment == 0,
        "Buffer pointer is not properly aligned"
    );
}

#[test]
fn test_mmap_buffer_multiple_allocations() {
    let sizes = [512, 1024, 4096, 8192];
    let mut buffers = Vec::new();

    // Allocate multiple buffers
    for &size in &sizes {
        let buffer = MmapBuffer::allocate_regular(size).expect("Failed to allocate buffer");
        assert_eq!(buffer.len(), size);
        assert!(!buffer.ptr().is_null());
        buffers.push(buffer);
    }

    // Verify all buffers are still valid
    for (i, buffer) in buffers.iter().enumerate() {
        assert_eq!(buffer.len(), sizes[i]);
        assert!(!buffer.ptr().is_null());
    }

    // Buffers will be dropped automatically
}

#[test]
fn test_mmap_buffer_large_allocation() {
    let large_size = 10 * 1024 * 1024; // 10MB
    let mut buffer =
        MmapBuffer::allocate_regular(large_size).expect("Failed to allocate large buffer");

    assert_eq!(buffer.len(), large_size);
    assert!(buffer.capacity() >= large_size);
    assert!(!buffer.ptr().is_null());

    // Test writing to the beginning, middle, and end
    {
        let bytes_mut = buffer.as_bytes_mut();
        bytes_mut[0] = 1;
        bytes_mut[large_size / 2] = 2;
        bytes_mut[large_size - 1] = 3;
    }

    assert_eq!(buffer[0], 1);
    assert_eq!(buffer[large_size / 2], 2);
    assert_eq!(buffer[large_size - 1], 3);
}
