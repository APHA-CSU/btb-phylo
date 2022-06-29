#!/bin/sh

set -e

wget https://megasoftware.net/do_force_download/mega-cc_11.0.13-1_amd64.deb
#apt-get install -y ./mega-cc_11.0.13-1_amd64.deb
sudo dpkg -i mega-cc_11.0.13-1_amd64.deb
rm mega-cc_11.0.13-1_amd64.deb
