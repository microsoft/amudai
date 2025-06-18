# `amudai-codegentool`: code-generation tasks for Amudai development

## Building `amudai-codegentool`

From the repo root:

```
cargo build --release -p amudai-codegentool
```

```
./target/release/amudai-codegentool --help
```

## Re-generate Protobuf definitions

```
./target/release/amudai-codegentool generate-proto
```

## Re-generate Flatbuffers definitions

```
./target/release/amudai-codegentool generate-flatbuffers
```
