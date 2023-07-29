#!/bin/bash

#created: 230221
#modification
	# 230606: added default argument to --rserverdir (in Chicago)
	# 230619: removes all contents in "workingdir" at the beginning

#adopted from /opt/rstudio/run_here.sh

# must be run with sudo previlege

set -eu

# TIPS #
# rstudio_basedir must be within "/opt/rstudio/". rstudio does not work when the basedir was under "~pjh/tools/rstudio-server".
########

function usage {
    echo "Arguments:"
    echo -e "\t--port: Port number"
    #echo -e "\t--workingdir: Rstudio server working directory. It must be empty. \"/var/run/rstudio-server\" directory tree is automatically created under this directory."
    echo -e "\t--workingdir: Rstudio server working directory. \"/var/run/rstudio-server\" directory tree is automatically created under this directory."
    echo -e "\t--condapf: Conda environment directory, which looks like \"*/miniconda3/envs/<envname>\""
    echo -e "\t--rserverdir: (OPTIONAL) Rstudio server top directory, which looks like \"*/usr/lib/rstudio-server\""
    exit 1
}

function argparse {
    declare -gA defaults
    defaults[rserverdir]="/home/users/pjh/tools/rstudio-server/rstudio-server-rhel-1.4.1717/usr/lib/rstudio-server/"

    required_args=(
        port
        workingdir
        condapf
        #rserverdir
    )

    # main
    declare -gA args
    if [[ $# = 0 ]] ; then
        usage
    else
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
    fi

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
    if [[ ! ${args[condapf]} =~ /miniconda3/envs/ ]] ; then
        echo "Invalid \"--condapf\" argument pattern."
        exit 1
    fi
    if [[ ! ${args[rserverdir]} =~ /usr/lib/rstudio-server?/$ ]] ; then
        echo "Invalid \"--rserverdir\" argument pattern."
        exit 1
    fi


    if [[ -e ${args[workingdir]} ]] ; then
        if [[ -d ${args[workingdir]} ]] ; then
            echo "A directory with the name specified by \"--workingdir\" (${args[workingdir]}) argument exists."
            echo "If you enter 'yes', the existing directory tree will be removed and the program will proceed."
            echo "If you enter anything other, the program will abort."
            read input
            if [[ $input = yes ]] ; then
                rm -rf ${args[workingdir]}
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


# main
argparse "$@"

#rstudio_basedir=/opt/rstudio/other_version/rstudio-server-rhel-1.4.1725/usr/lib/rstudio-server/
rstudio_basedir=${args[rserverdir]}   # 1.4.1717 works! 1.4.1725 easily fails!
rserver_path=${rstudio_basedir}/bin/rserver
rsession_path=${rstudio_basedir}/bin/rsession

R_path=${args[condapf]}/bin/R
lib_path=${args[condapf]}/lib # contains libz.so.1 which is required for plotting in Rstudio

port=${args[port]}
rstudio_server_user=pjh

WD=${args[workingdir]}
mkdir -p $WD


# set environments
export LD_LIBRARY_PATH=${lib_path}
reticulate_env="$(dirname ${args[condapf]})/r-reticulate"
export RETICULATE_PYTHON=${reticulate_env}/bin/python3.6


# set bg_pids
declare -a bg_pids


# set log file
logpath=${args[workingdir]}/log
echo -n > $logpath  # truncates existing one


# write beginning timestamp
echo [$(date)] BEGINNING >> $logpath
echo [$(date)] $(declare -p args) >> $logpath


# periodically print cmdline arguments to STDOUT
(
    while true ; do
        echo
        echo [$(date)] $(declare -p args)
        echo
        sleep 3600
    done
) &
bg_pids+=($!)


# run main program
(
    $rserver_path \
        --server-user $rstudio_server_user \
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
sigint_handler() {
    for pid in ${bg_pids[@]} ; do
        kill $pid
    done
}


trap sigint_handler SIGINT
sleep infinity
