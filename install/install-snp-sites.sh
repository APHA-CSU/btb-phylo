#!/bin/sh

set -e

ENV=${1:-native}

if [ $ENV == "native" ]; then
    sudo apt-get install -y snp-sites
else
    apt-get install -y snp-sites
fi
