#!/bin/bash

set -e

################## ARGS ##############################

BIOTOOLS_PATH=~/biotools/

################## DEPENDENCIES ######################

apt-get -y update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    git \
    gcc \
    make \
    python3 \
    python3-pip
# for snp-sites source install
    #check \
    #libtool \
    #autoconf \

echo $PWD
pip3 install -r btb-phylo/requirements.txt
ln -s /usr/bin/python3 /usr/bin/python

################## BIOTOOLS ######################


mkdir -p $BIOTOOLS_PATH
cp ./install*.*sh $BIOTOOLS_PATH
cd $BIOTOOLS_PATH

bash -e install-snp-sites.sh
bash -e install-snp-dists.sh
