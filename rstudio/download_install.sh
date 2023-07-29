#!/usr/bin/bash
set -eu


function usage {
    echo -e "Description:"
    echo -e "\tIntended for CentOS, installing from .rpm file."
    echo -e "\trpm2cpio and cpio must be installed."
    echo -e "\trstudio server file tree will be written to --outdir directory. It would be a single directory named '/usr'."
    echo -e "\tDefault rstudio-server rpm file version is 1.4.1717 ; this is felt to be stable."
    echo -e
    echo -e "Usage: $(basename $0) \\"
    echo -e "\t[optional] --rpmfile : Path to rstudio-server .rpm file. If there is not a local file named as such, it is regarded as a url for remote file, and will be downloaded to 'outdir'. Default: ${defaults[rpmfile]}"
    echo -e "\t[optional] --outdir : Directory where rstudio server file tree will be written. If this is an existing directory, it must be empty. Default: ${defaults[outdir]}"
    exit 1
}


function argparse {
    declare -gA defaults
    defaults[outdir]="."
    defaults[rpmfile]="https://download2.rstudio.org/server/centos7/x86_64/rstudio-server-rhel-1.4.1717-x86_64.rpm"

    required_args=(
    )

    # main
    declare -gA args
    while [[ $# -gt 0 ]] ; do
        case "$1" in
            --outdir)
                shift ; args[outdir]="$1" ; shift ;;
            --rpmfile)
                shift ; args[rpmfile]="$1" ; shift ;;
            -h|--help|*)
                usage ;;
        esac
    done

    # check if required arguments are all set
    for key in ${required_args[@]:-} ; do
        if [[ -z ${args[$key]:-} ]] ; then
            echo "Required argument --${key} is not set."
            exit 1
        fi
    done

    # setting default args
    if [[ ${#defaults[@]} -gt 0 ]] ; then
        for key in ${!defaults[@]} ; do
            args[$key]=${args[$key]:-${defaults[$key]}}
        done
    fi
    
    # sanity check
    if [[ -e ${args[outdir]} ]] ; then
        if [[ ! -d ${args[outdir]} ]] ; then
            echo "'--outdir' is an already existing non-directory file."
            exit 1
        else
            if [[ $( ls ${args[outdir]} | wc -l ) != 0 ]] ; then
                echo "'--outdir' is an already existing non-empty directory. It must be empty."
                exit 1
            fi
        fi
    fi
}


function main {
    # make outdir if absent
    if [[ ! -e ${args[outdir]} ]] ; then
        mkdir ${args[outdir]}
    fi

    # move to outdir
    initdir=$(realpath -s $(pwd))
    echo $initdir ; exit
    cd ${args[outdir]}

    # download rpm file if absent
    if [[ ! -e ${args[rpmfile]} ]] ; then
        rpmpath=./$(basename ${args[rpmfile]})
        wget -O $rpmpath ${args[rpmfile]}
    else
        rpmpath=${args[rpmfile]}
    fi

    # extract files from rpm
    rpm2cpio $rpmpath | cpio -idmv

    # return to the starting directory
    cd $initdir
}


# main
argparse "$@"
main

