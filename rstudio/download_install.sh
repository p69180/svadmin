#!/bin/bash
#210802
set -eu

download_path=https://s3.amazonaws.com/rstudio-ide-build/server/centos7/x86_64/rstudio-server-rhel-1.4.1725-x86_64.rpm
fname=$(basename $download_path)

wget $download_path
rpm2cpio $fname | cpio -idmv
