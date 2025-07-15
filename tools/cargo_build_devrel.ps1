$RustFlagsValue = '-C target-cpu=native'

try {
    $env:RUSTFLAGS = $RustFlagsValue
    & cargo build --profile devrel
}
finally {
    Remove-Item Env:RUSTFLAGS -ErrorAction SilentlyContinue
}

exit $cargoExitCode
