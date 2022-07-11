#!/bin/sh

set -e

wget https://www.megasoftware.net/do_force_download/megax-cc_10.2.6-1_amd64.deb
apt-get install -y ./mega-cc_10.2.6-1_amd64.deb
rm mega-cc_11.0.13-1_amd64.deb
