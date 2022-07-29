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
    python3-pip \
    wget \
    awscli

ln -s /usr/bin/python3 /usr/bin/python
python setup.py install

################## BIOTOOLS ##########################

mkdir -p $BIOTOOLS_PATH
cp ./install/install*.*sh $BIOTOOLS_PATH
cd $BIOTOOLS_PATH

bash -e install-snp-sites.sh docker
bash -e install-snp-dists.sh docker
bash -e install-megacc.sh docker
