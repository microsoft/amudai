# amudai-encoding

This crate implements various encodings for data serialization and deserialization. It provides efficient and reliable methods to encode and decode data in multiple formats, ensuring compatibility and performance. The crate supports a wide range of encoding schemes, making it suitable for diverse applications, including data storage, transmission, and processing.

## Ensuring SIMD optimizations

Most of the encoding methods are implemented in a way that allows LLVM to generate SIMD code.
To ensure that the generated code is indeed optimized, `cargo-show-asm` plug-in may be used.

First, the plug-in needs to be installed:

```bash
cargo install cargo-show-asm
```

To view all the available functions, run:

```bash
RUSTFLAGS='-C target-cpu=native' cargo asm --release -p amudai-encodings --rust --lib
```

To view the assembly code of a specific function, add its index to the previous command:

```bash
RUSTFLAGS='-C target-cpu=native' cargo asm --release -p amudai-encodings --rust --lib 14
```
