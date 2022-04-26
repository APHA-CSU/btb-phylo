#!/bin/sh

set -e

git clone https://github.com/tseemann/snp-dists.git
cd snp-dists
make
ln -s $PWD/snp-dists /usr/local/bin/snp-dists
