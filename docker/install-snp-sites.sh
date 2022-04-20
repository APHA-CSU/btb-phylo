#!/bin/sh

set -e

git clone https://github.com/sanger-pathogens/snp-sites.git
cd snp-sites
autoreconf -i -f
./configure
make
ln -s $PWD/snp-sites /usr/local/bin/snp-sites
