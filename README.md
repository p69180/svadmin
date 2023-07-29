This repository contains utility scripts for server administration.

## rstudio server launcher
### scripts
- [rstudio/download_install.sh](rstudio/download_install.sh)
  - prerequisite: wget, rpm2cpio, cpio (probably installed on CentOS)
  - What it does: Download Rstudio server rpm file, if one is not already downloaded. Then, extracts files from the rpm.
  - Options
    - --rpmfile : (optional) Path to local rpm file, or remote file url. Default is url for 1.4.1717 version.
    - --outdir : (optional) The directory where Rstudio file tree will be written. Default is the current working directory.

### TIPS
- 1.4.1717 works! 1.4.1725 easily fails!

### How to setup
1. Download and extract Rstudio server rpm file using [rstudio/download_install.sh](rstudio/download_install.sh).
   - You can specify url of rpm file of a specific version with "--rpmfile" option.
   - Links for rstudio rpm files can be found from https://global.rstudio.com/products/rstudio/older-versions/
   - By default, 1.4.1717 is used.
   
