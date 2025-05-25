In order to read this book, you will need to install a number of Rust tools:

* mdbook
* mdbook-admonish
* mdbook-mermaid
* mdbook-toc

So, make sure that you have a recent Rust toolset installed, then run:

```batch
cargo install mdbook

cargo install mdbook-admonish
mdbook-admonish install .

cargo install mdbook-mermaid
mdbook-mermaid install .

cargo install mdbook-toc
```

(Replace `.` above with the full path to the `Amudai\docs\spec` directory if you are not running from that directory.)

To read, use (on Windows):

```cmd
read.cmd
```

TODO: Running this on Linux
