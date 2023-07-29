#!/usr/bin/bash
# must be run with sudo previlege

set -eu

function usage {
    echo -e "Description:" 
    echo -e "\tMust be run with root previlege." 
    echo -e "\tRstudio server instance is created using the R installation from given conda environment."
    echo -e "\tRunning status (including initial arguments) is periodically printed to stdout every 1 hour."
    echo -e "\tLog file is created at <workingdir>/log"
    echo -e
    echo -e "Arguments:"
    echo -e "\t--port: The port which rstudio server will use."
    echo -e "\t--workingdir: Rstudio server working directory. \"/var/run/rstudio-server\" directory tree is automatically created under this directory."
    echo -e "\t--condapf: Conda environment directory, which looks like \"*/miniconda3/envs/<envname>\""
    echo -e "\t--rserverdir: Rstudio server top directory, which looks like \"*/usr/lib/rstudio-server\""
    exit 1
}


function rootwarn {
    if [[ $USER != root ]] ; then
        echo "You must be a root user."
        exit 1
    fi
}


function argparse {
    declare -gA defaults

    required_args=(
        port
        workingdir
        condapf
        rserverdir
    )

    # do argument parsing
    declare -gA args
    while [[ $# -gt 0 ]] ; do
        case "$1" in
            --port)
                shift ; args[port]="$1" ; shift ;;
            --workingdir)
                shift ; args[workingdir]="$1" ; shift ;;
            --condapf)
                shift ; args[condapf]="$1" ; shift ;;
            --rserverdir)
                shift ; args[rserverdir]="$1" ; shift ;;
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
    if [[ ! $(realpath -s ${args[condapf]}) =~ /(miniconda3|anaconda3)/envs/[^/]+/?$ ]] ; then
        echo "Invalid \"--condapf\" argument pattern. Its absolute path form must be something like '*/miniconda3/envs/myenv'"
        exit 1
    fi
    if [[ ! $(realpath -s ${args[rserverdir]}) =~ /usr/lib/rstudio-server/?$ ]] ; then
        echo "Invalid \"--rserverdir\" argument pattern. Its absolute path form must be something like '*/usr/lib/rstudio-server'"
        exit 1
    fi
}


function WD_handler {
    if [[ -e ${args[workingdir]} ]] ; then
        if [[ -d ${args[workingdir]} ]] ; then
            echo "A directory with the name specified by \"--workingdir\" (${args[workingdir]}) argument exists."
            echo "If you enter 'yes', the existing directory will be removed and a new directory with the same name will be created."
            echo "If you enter anything other, the program will quit."
            read input
            if [[ $input = yes ]] ; then
                rm -rf ${args[workingdir]}
                mkdir ${args[workingdir]}
            else
                exit 1
            fi
        else
            echo "There exists a non-directory file with the name specified by \"--workingdir\" argument."
            exit 1
        fi
    else
        mkdir ${args[workingdir]}
    fi
}


function main {
    # parameter setting
    rstudio_basedir=${args[rserverdir]}   # 1.4.1717 works! 1.4.1725 easily fails!
    rserver_path=${rstudio_basedir}/bin/rserver
    rsession_path=${rstudio_basedir}/bin/rsession

    R_path=${args[condapf]}/bin/R
    lib_path=${args[condapf]}/lib # contains libz.so.1 which is required for plotting in Rstudio

    port=${args[port]}
    #rstudio_server_user=pjh

    WD=${args[workingdir]}


    # set environments
    export LD_LIBRARY_PATH=${lib_path}

    # reticulate
    #reticulate_env="$(dirname ${args[condapf]})/r-reticulate"
    #export RETICULATE_PYTHON=${reticulate_env}/bin/python3.6


    # set log file
    logpath=${args[workingdir]}/log
    echo -n > $logpath  # truncates existing one


    # write beginning timestamp
    echo [$(date)] BEGINNING >> $logpath
    echo [$(date)] $(declare -p args) >> $logpath


    ## periodically print cmdline arguments to STDOUT
    declare -a bg_pids
    (
        while true ; do
            echo
            echo "[$(date)] $(declare -p args) ; logpath=${logpath}"
            echo
            sleep 3600
        done
    ) &
    bg_pids+=($!)

    ## run main program
            #--server-user $rstudio_server_user \
    (
        $rserver_path \
            --server-working-dir=/ \
            --server-daemonize=0 \
            --server-pid-file=${WD}/var/run/rstudio-server.pid \
            --server-data-dir=${WD}/var/run/rstudio-server \
            --www-address=0.0.0.0 \
            --www-port=${port} \
            --auth-none=0 \
            --rsession-path=${rsession_path} \
            --database-config-file ${WD}/database.conf \
            --rsession-which-r=${R_path} \
            &>> $logpath
    ) &
    bg_pids+=($!)


    # set sigint handler
    function sigint_handler {
        for pid in ${bg_pids[@]} ; do
            kill $pid
        done
    }

    trap sigint_handler SIGINT
    sleep infinity
}


# main
rootwarn
argparse "$@"
WD_handler
main

