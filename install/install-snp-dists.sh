#!/bin/sh

set -e

ENV=${1:-native}

echo $ENV

git clone https://github.com/tseemann/snp-dists.git
cd snp-dists
make
if [ $ENV == "native" ]; then
    echo "foo"
    sudo ln -s $PWD/snp-dists /usr/local/bin/snp-dists
else
    echo "bar"
    ln -s $PWD/snp-dists /usr/local/bin/snp-dists
fi
