#!/bin/bash

set -e

################## ARGS ##############################

BIOTOOLS_PATH=~/biotools/

################## DEPENDENCIES ######################

sudo apt-get -y update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    git \
    gcc \
    make \
    wget \
    awscli

################## BIOTOOLS ##########################

mkdir -p $BIOTOOLS_PATH
cp ./install/install*.*sh $BIOTOOLS_PATH
cd $BIOTOOLS_PATH

bash -e install-snp-sites.sh
bash -e install-snp-dists.sh
bash -e install-megacc.sh
