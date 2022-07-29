#!/bin/sh

set -e

ENV=${1:-local}

if [ $ENV == "local" ]; then
    sudo apt-get install -y snp-sites
elif [ $ENV == "docker" ]; then
    apt-get install -y snp-sites
fi
