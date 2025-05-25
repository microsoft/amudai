# `codegentool`: code-generation tasks for Amudai development

## Building `codegentool`

From the repo root:

```
cargo build --release -p codegentool
```

```
./target/release/codegentool --help
```

## Re-generate Protobuf definitions

```
./target/release/codegentool generate-proto
```

## Re-generate Flatbuffers definitions

```
./target/release/codegentool generate-flatbuffers
```
