This repository contains utility scripts for server administration.

## rstudio server launcher
### Scripts
- All scripts can be run with "-h" option to show help message.

- [rstudio/download_install.sh](rstudio/download_install.sh)
  - prerequisite: wget, rpm2cpio, cpio (probably installed on CentOS)
  - Arguments
    - --rpmfile : (optional) Path to local rpm file, or remote file url. Default is url for 1.4.1717 version. This version was tested for quite a long time and felt to be stable.
    - --outdir : (optional) The directory where Rstudio file tree will be written. Default is the current working directory.
  - What it does
    - Downloads Rstudio server rpm file, if one is not already downloaded. Then, extracts files from the rpm.
    - In the target directory (--outdir argument), a new directory named `usr` will be created, where Rstudio files are contained.

- [rstudio/run_after_env_creation.sh](rstudio/run_after_env_creation.sh)
  - What it does: Modification of installed conda environment, which includes:
      - Writing a custom-made `Rprofile.site` file
        - This creates a user-specific R package installation directory and adds it to the library trees.
      - Writing a custom-made `Renviron.site` file
        - This creates **RETICULATE_PYTHON** environment variable, required for running reticulate package.
      - Modifying `Makeconf` file
        - This forces R to use C libraries installed within conda environment, facilitating new package installation.
  - Arguments
    - --condapf : Conda environment directory path

- [rstudio/run_conda_rstudio.sh](rstudio/run_conda_rstudio.sh)
  - What it does: Launches an rstudio server instance using a specific conda environment.
  - ***Must be run with root previlege!!***
  - Arguments
    - --port : Set an appropriate port number
    - --workingdir : This need not be created in advance. If already existing, the existing directory is deleted, then a new one with the same name is created.
    - --condapf : Conda environment directory path (e.g. `/path/to/miniconda3/envs/myenv`)
    - --rserverdir : Path to the rstudio server directory. This should look like: `/path/to/rstudio-install-dir/usr/lib/rstudio-server`

### TIPS
- 1.4.1717 works! 1.4.1725 easily fails!

### How to setup
1. Download and extract Rstudio server rpm file using [rstudio/download_install.sh](rstudio/download_install.sh).
   - You can specify url of rpm file of a specific version with "--rpmfile" option.
   - Links for rstudio rpm files can be found from https://global.rstudio.com/products/rstudio/older-versions/
   - Example
     ```
     # bash script
     # Suppose you are currently at the root of this repository
     
     rstudiodir=/where/to/install/rstudio_server
     mkdir $rstudiodir
     ./rstudio/download_install.sh --outdir $rstudiodir
     ```
2. Create a conda environment which contains R and R packages.
3. Execute [rstudio/run_after_env_creation.sh](rstudio/run_after_env_creation.sh) to modify the conda environment.
   - Example
     ```
     # Continued from the above example code
     
     # Suppose miniconda3 was used and the name of env is "condaR"
     condapf=/path/to/miniconda3/envs/condaR
     ./rstudio/run_after_env_creation.sh --condapf $condapf
     ```
 4. Run Rstudio server with [rstudio/run_conda_rstudio.sh](rstudio/run_conda_rstudio.sh)
    - The path to rstudio server directory will be `<rstudiodir>/usr/lib/rstudio-server`. \<rstudiodir\> is the argument used when running [rstudio/download_install.sh](rstudio/download_install.sh)
    - Example
      ```
      # Continued from the above example code
      
      rserverdir=${rstudiodir}/usr/lib/rstudio-server
      workingdir=/path/to/rstudio/workingdir
      ./rstudio/run_conda_rstudio.sh \
        --port 12345 \
        --workingdir $workingdir \
        --condapf $condapf \
        --rserverdir $rserverdir
      ```
5. Login to Rstudio server from remote host
   - Access to the machine where Rstudio is running, with the port number used when running [rstudio/run_conda_rstudio.sh](rstudio/run_conda_rstudio.sh)


