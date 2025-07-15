@ECHO  OFF
SETLOCAL
PUSHD %~dp0
START mdbook serve --open --dest-dir %TEMP%\mdbook-amudai
