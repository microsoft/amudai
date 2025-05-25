#!/bin/sh 
_non_interactive=0
while [ $# -gt 0 ]; do
  case "$1" in
  --non-interactive)
    _non_interactive="1"
    shift 1
    ;;
  --help | -h)
    echo "USAGE: cargologin.sh [--non-interactive]"
    echo "  --non-interactive - (optional) don'n run az login automatically if the no active login detected"
    exit 0
    ;;
  *)
    echo "Invalid argument: $1"
    exit 1
    ;;
  esac
done



function check_az_logged_in() {
    local _token_exp_time_s=$(az account get-access-token --query "expiresOn" --output tsv 2> /dev/null)
    if [ $? != 0 ] || [ -z "$_token_exp_time_s" ]
    then
        return 1
    fi
    local _now=$(date +"%s")
    local _token_exp_time=$(date --date "$_token_exp_time_s" +"%s")
    if [ $_token_exp_time -gt $_now ]
    then
        return 0
    fi
    return 1    
}

if command -v cargo &> /dev/null
then
    _cargo=$(command -v cargo)
else
    if [ -e $HOME/.cargo/bin/cargo ]
    then
        if [ -e $HOME/.cargo/env ]
        then
            source $HOME/.cargo/env
        else
            echo "There is cargo binary in $HOME/.cargo/bin but no $HOME/.cargo/env - please make sure rust is properly set up"
            exit 1
        fi
        _cargo=$HOME/.cargo/bin/cargo
    fi
fi

if [ -z $_cargo ]
then
    echo "cargo command is not in the PATH and not in $HOME/.cargo/bin - please make sure rust is properly set up"
    exit 1
fi

if ! check_az_logged_in
then
    if [ "$_non_interactive" -eq "1" ]
    then
        exit 0
    fi
    set -e
    az login
fi
set -e    
az account get-access-token --query "join(' ', ['Bearer', accessToken])" --output tsv | $_cargo login --registry Azure-Kusto-Service_PublicPackages
