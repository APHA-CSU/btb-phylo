#!/bin/sh

set -e

ENV=${1:-native}

if [ $ENV == "native" ]; then
    wget https://www.megasoftware.net/do_force_download/megax-cc_10.2.6-1_amd64.deb
    apt-get install -y ./megax-cc_10.2.6-1_amd64.deb
    rm megax-cc_10.2.6-1_amd64.deb
else
    wget https://megasoftware.net/do_force_download/mega-cc_11.0.13-1_amd64.deb
    apt-get install -y ./mega-cc_11.0.13-1_amd64.deb
    rm mega-cc_11.0.13-1_amd64.deb
fi
