#!/bin/bash
# by pjh
# last update 221216

set -eu

# argparse
declare -A defaults
defaults[minsize]=500M
defaults[sleepsec]=5
defaults[waitcount]=10
defaults[tmpdir]=/home/tmp

usage() {
    echo "Usage: $(basename $0) \\"
    echo $'\t'"--topdir : Top directory to begin file search. \\"
    echo $'\t'"--minsize : default ${defaults[minsize]} \\"
    echo $'\t'"--sleepsec : default ${defaults[sleepsec]} \\"
    echo $'\t'"--waitcount : default ${defaults[waitcount]}"
    echo $'\t'"--tmpdir : default ${defaults[tmpdir]}"
    exit 1
}

declare -A args
while [[ $# -gt 0 ]] ; do
    case "$1" in
        --topdir)
            shift
            args[topdir]="$1"
            shift
            ;;
        --minsize)
            shift
            args[minsize]="$1"
            shift
            ;;
        --sleepsec)
            shift
            args[sleepsec]="$1"
            shift
            ;;
        --waitcount)
            shift
            args[waitcount]="$1"
            shift
            ;;
        --tmpdir)
            shift
            args[tmpdir]="$1"
            shift
            ;;
        *|-h|--help)
            usage
            ;;
    esac
done


# set default args
for key in ${!defaults[@]} ; do
    args[$key]=${args[$key]:-${defaults[$key]}}
done


# check if user is root
if [[ $(id -u) != 0 ]]
then
    echo "Only root user can run this script."
    exit 1
fi


# define functions
declare -A tmpfiles
for x in bnode{0..16} ; do
    tmpfiles[$x]=$(mktemp -p ${args[tmpdir]})
done


check_file_is_open() {
    for key in ${!tmpfiles[@]} ; do
        ssh $key "lsof \"$1\" &> /dev/null ; echo -n \$?" > ${tmpfiles[$key]} &
    done
    wait

    opened=false
    for filename in ${tmpfiles[@]} ; do
        if [[ $(cat $filename) = 0 ]]
        then
            opened=true
            break
        fi
    done

    if $opened
    then
        return 0
    else
        return 1
    fi
}


rm_tmpfiles() {
    for fname in ${tmpfiles[@]} ; do
        rm -f "$fname"
    done
}


run_by_file() {
    topdir="$1"
    minsize="$2"
    sleepsec="$3"
    waitcount="$4"
    tmpdir="$5"

    find "$topdir" -type f -size +${minsize} -uid +1000 | 
        while read fname ; do
            echo "[$(date)] Beginning handling of the file $fname"

            # 1-1) skip if file is open by some processes
            if check_file_is_open "$fname"
            then
                echo "[$(date)] Skipping because the file was open at the first encounter"
                continue
            else
                echo "[$(date)] Confirmed the file is not open by any process"
            fi

            # 1-2) compare file size to tmpdir space before going further
            filesize=$(stat --printf=%s "$fname")
            tmpdir_avail_size=$(df "$tmpdir" | awk 'NR==2{print $4}')
            if [[ $(bc <<< "$filesize > $tmpdir_avail_size * 0.4") = 1 ]]
            then
                echo "[$(date)] Skipping because the file size is greater than the available temporary directory space"
                continue
            else
                echo "[$(date)] Confirmed the file size is small enough compared to the available temporary directory space"
            fi

            # 2)
            copy_path=$(mktemp -p "$tmpdir")
            tmp_path=$(mktemp -p "$tmpdir")
            echo "[$(date)] BEGINNING copying"
            echo "[$(date)] copy_path: $copy_path"
            echo "[$(date)] tmp_path: $tmp_path"
            # 3)
            cp -p "$fname" "$copy_path"
            echo "[$(date)] FINISHED copying"

            # 4) Check again if file is open
            loop_count=0
            skipped=false
            while true ; do
                let ++loop_count

                if check_file_is_open "$fname"
                then
                    # 4-2) Wait until the file is not open by any process
                    if [[ $loop_count -gt $waitcount ]]
                    then
                        echo "[$(date)] Skipping because waiting loop count exceeded limit"
                        skipped=true
                        break
                    fi

                    echo "[$(date)] (loop count $loop_count) Waiting for the file to be closed."
                    sleep $sleepsec
                    continue
                else
                    # 4-1)
                    break
                fi
            done

            if $skipped
            then
                echo "[$(date)] Removing copy of the original file"
                rm -f "$copy_path"
            else
                # 5)
                echo "[$(date)] BEGINNING - removing original file and renaming copy as original file"
                mv "$fname" "$tmp_path"
                mv "$copy_path" "$fname"
                echo "[$(date)] FINISHED - removing original file and renaming copy as original file"
                rm -f "$tmp_path"
                echo "[$(date)] Successfully handled file $fname"
            fi

            echo
        done
}


# main
run_by_file "${args[topdir]}" ${args[minsize]} ${args[sleepsec]} ${args[waitcount]} "${args[tmpdir]}"
rm_tmpfiles
