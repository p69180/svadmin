#!/bin/bash
# by pjh
# last update 230112
# last update 230711

set -eu

# argparse
declare -A defaults
defaults[topdir]=/home/users
defaults[ostidx]=0
defaults[minsize]=100M
defaults[tmpdir]=/home/tmp
defaults[parallel]=1
defaults[mode]="find"

usage() {
    echo "Usage: $(basename $0) \\"
    echo $'\t'"--mode : Must be either 'find' or 'lfs_find'. Default: ${defaults[mode]} \\"
    echo
    echo $'\t'"--topdir : Top directory to begin file search. Only relevant when --mode is 'find'. Default: ${defaults[topdir]} \\"
    echo $'\t'"--ostidx : Index of OST to do file search. Only relevant when --mode is 'lfs_find'. Default: ${defaults[ostidx]} \\"
    echo
    echo $'\t'"--minsize : Only files greater than this value are migrated. Default: ${defaults[minsize]} \\"
    echo $'\t'"--parallel : Migrate at most this many files at a time. Used as -P argument for xargs. Default: ${defaults[parallel]} \\"
    echo
    echo $'\t'"--tmpdir : Not used. Default: ${defaults[tmpdir]} \\"
    exit 1
}

declare -A args
while [[ $# -gt 0 ]] ; do
    case "$1" in
        --topdir)
            shift ; args[topdir]="$1" ; shift ;;
        --ostidx)
            shift ; args[ostidx]="$1" ; shift ;;
        --minsize)
            shift ; args[minsize]="$1" ; shift ;;
        --tmpdir)
            shift ; args[tmpdir]="$1" ; shift ;;
        --parallel)
            shift ; args[parallel]="$1" ; shift ;;
        --mode)
            shift ; args[mode]="$1" ; shift ;;
        *|-h|--help)
            usage
            ;;
    esac
done


# set default args
for key in ${!defaults[@]} ; do
    args[$key]=${args[$key]:-${defaults[$key]}}
done


# check if required arguments are all set
required_args=(
)
for key in ${required_args[@]:-} ; do
    if [[ -z ${args[$key]:-} ]] ; then
        echo "Required argument --${key} is not set."
        exit 1
    fi
done


# other argument sanity checks
if [[ (${args[mode]} != lfs_find) && (${args[mode]} != find) ]] ; then
    echo "--mode argument must be either 'find' or 'lfs_find'."
    exit 1
fi


# check if user is root
if [[ $(id -u) != 0 ]]
then
    echo "You are not a root user. You cannot migrate files not owned by you."
    #echo "Only root user can run this script."
    #exit 1
fi


# define functions
#declare -A ost_indexes
#while IFS=" " read -a linesp ; do
#    idx=$(tr -d ":" <<< ${linesp[0]})
#    uuid=${linesp[1]}
#    active=${linesp[2]}
#    ost_indexes[$uuid]=$idx
#done < <(lfs osts | tail -n +2)

ost_idx_with_most_space() {
    ost_idx=$(
        lfs df | 
            awk '
            $NF ~ /\[OST:.*\]$/ {print}' |
            sort -k4,4nr | 
            head -n1 | 
            awk '{print gensub("^.*\\[OST:(.+)\\]$", "\\1", "g", $NF)}'
    )
    echo $ost_idx
}


ost_idx_with_most_space_from_lfsinfo_pct() {
    # Select ost with the least occupancy percent

    for key in ${!lfsinfo_pct[@]} ; do
        if [[ $key = "sum" || $key =~ "MDT" ]] ; then
            continue
        fi
        echo $key ${lfsinfo_pct[$key]}
    done | 
        sort -k2,2n | 
        head -n1 | 
        awk '{print gensub("OST:", "", "g", $1)}'
}


ost_idx_with_most_space_from_lfsinfo_avail() {
    # Select ost with the most available space

    for key in ${!lfsinfo_avail[@]} ; do
        if [[ $key = "sum" || $key =~ "MDT" ]] ; then
            continue
        fi
        echo $key ${lfsinfo_avail[$key]}
    done | 
        sort -k2,2nr | 
        head -n1 | 
        awk '{print gensub("OST:", "", "g", $1)}'
}


parse_lfs_df() {
    declare -gA lfsinfo_pct
    declare -gA lfsinfo_avail
    while read line ; do
        read -a linesp <<< "$line"

        if [[ (${linesp[0]:-} = "UUID") || -z ${linesp[0]:-} ]]
        then 
            continue
        fi

        pct=$(tr -d '%' <<< ${linesp[4]})
        avail=${linesp[3]}

        if [[ ${linesp[5]} = "/home/users" ]]
        then
            key="sum"
        else
            key=$(awk '{print gensub("^.*\\[(.+)\\]$", "\\1", "g", $0)}' <<< ${linesp[5]})
        fi

        lfsinfo_pct[$key]=$pct
        lfsinfo_avail[$key]=$avail
    done < <(lfs df)
}


migrate_after_check() {
    fname="$1"
    tmpdir="$2"
    file_ost_idx="$3"

    # make a backup
    echo "[$(date)] Making a backup file"
    copy_path=$(mktemp -p "$tmpdir")
    echo "[$(date)] copy_path: $copy_path"
    cp -p "$fname" "$copy_path"

    spacious_ost_idx=$(ost_idx_with_most_space_from_lfsinfo_avail)
    echo "[$(date)] Beginning lfs migrate; migrating from OST index $file_ost_idx to $spacious_ost_idx"
    if lfs migrate -o $spacious_ost_idx -b "$fname"
    then
        echo "[$(date)] Finished lfs migrate"
        echo "[$(date)] Checking identity of the backup file and migrated file"

        ###########
        # run cmp #
        ###########

        n_loop=0
        while true ; do
            let ++n_loop

            if [[ $n_loop -gt 10 ]] ; then
                echo "[$(date)] The number of \"cmp\" trial exceeded 10. Assumes that two files are not identical."
                identical=false
                break
            fi
            
            echo "[$(date)] Running \"cmp\" (trial $n_loop)"
            cmp "$fname" "$copy_path" || :
            if [[ $? = 0 ]] ; then
                identical=true
                break
            elif [[ $? = 1 ]] ; then
                identical=false
                break
            elif [[ $? = 2 ]] ; then
                echo "[$(date)] \"cmp\" exited with a trouble. Retrying after sleep 60"
                sleep 60
                continue
            else
                echo "[$(date)] Exit status of \"cmp\" is neither 0, 1, nor 2." 1>&2
                exit 1
            fi
        done

        #####################################
        # Next step according to cmp result #
        #####################################

        if $identical
        then
            echo "[$(date)] Confirmed identity."
            rm -f "$copy_path"
        else
            echo "[$(date)] Migrated file differs from the original file. Renaming the copied file into the original file name."
            mv "$copy_path" "$fname"
        fi
    else
        #echo "[$(date)] lfs migrate failed because some processes are opening the file. Skipping this file."
        echo "[$(date)] lfs migrate failed. Skipping this file."
        rm -f "$copy_path"
    fi

    echo
}


migrate_without_check() {
    fname="$1"
    file_ost_idx="$2"

    #spacious_ost_idx=$(ost_idx_with_most_space_from_lfsinfo_avail)
    spacious_ost_idx=$(ost_idx_with_most_space_from_lfsinfo_pct)
    size_human=$(ls -lh "$fname" | awk '{print $5}')

    echo "[$(date)] Beginning lfs migrate of the file \"$fname\" (${size_human}) ; migrating from OST index $file_ost_idx to $spacious_ost_idx"
    if lfs migrate -o $spacious_ost_idx -b "$fname"
    then
        echo "[$(date)] Finished lfs migrate of the file \"$fname\" (${size_human})"
    else
        echo "[$(date)] lfs migrate failed. Skipping this file. (\"$fname\" (${size_human}))"
    fi

    echo
}


#run_by_file_old() {
#    topdir="$1"
#    minsize="$2"
#    tmpdir="$3"
#
#    find "$topdir" -type f -size +${minsize} -uid +1000 | 
#        while read fname ; do
#            # get params
#            parse_lfs_df  # now "lfsinfo_pct" and "lfsinfo_avail" is set
#
#            file_ost_idx=$(lfs getstripe -i "$fname")
#            occupancy_current=${lfsinfo_pct[OST:${file_ost_idx}]}
#            occupancy_sum=${lfsinfo_pct[sum]}
#
#            filesize=$(stat --printf %s "$fname")
#            tmpdir_size=$(df --output=avail ${args[tmpdir]} | tail -n1 | awk '{print $0 * 1024}')
#
#            # skip if current OST occupancy is less than gross mean
#            if [[ $occupancy_current -lt $occupancy_sum ]] 
#            then
#                continue
#            fi
#
#            # skip if file size is too large compared to available tmpdir size
#            if [[ $(bc <<< "${filesize} > ${tmpdir_size} * 0.75") = 1 ]]
#            then
#                continue
#            fi
#
#            # main
#            size_human=$(ls -lh "$fname" | awk '{print $5}')
#            echo "[$(date)] Beginning handling of the file \"$fname\" (${size_human})"
#
#            #migrate_after_check "$fname" "$tmpdir" "$file_ost_idx"
#            migrate_without_check "$fname" "$file_ost_idx"
#        done
#}


#run_by_file_new() {
#    target_ost_idx="$1"
#    minsize="$2"
#
#    lfs find --ost ${target_ost_idx} --type f --size +${minsize} /home/users |
#    #find "$topdir" -type f -size +${minsize} -uid +1000 | 
#        while read fname ; do
#            # get params
#            parse_lfs_df  # now "lfsinfo_pct" and "lfsinfo_avail" is set
#
#            occupancy_current=${lfsinfo_pct[OST:${target_ost_idx}]}
#            occupancy_sum=${lfsinfo_pct[sum]}
#
#            # break if target OST occupancy becomes less than gross mean
#            if [[ $occupancy_current -lt $occupancy_sum ]] ; then
#                break
#            fi
#
#            # main
#            size_human=$(ls -lh "$fname" | awk '{print $5}')
#            echo "[$(date)] Beginning handling of the file \"$fname\" (${size_human})"
#            migrate_without_check "$fname" "$target_ost_idx"
#        done
#}


run_by_file_find_mode() {
    fname="$1"

    # get params
    parse_lfs_df  # now "lfsinfo_pct" and "lfsinfo_avail" is set
    file_ost_idx=$(lfs getstripe -i "$fname")
    occupancy_current=${lfsinfo_pct[OST:${file_ost_idx}]}
    occupancy_sum=${lfsinfo_pct[sum]}

    # skip if current OST occupancy is less than gross mean
    if [[ $occupancy_current -gt $occupancy_sum ]] 
    then
        migrate_without_check "$fname" "$file_ost_idx"
    else
        sizestr=$(stat --printf "%s" $fname | numfmt --grouping)
        echo "[$(date)] Skipping file '$fname (size = $sizestr bytes)' because OST occupancy is lower than the gross mean."
    fi
}


run_by_file_lfsfind_mode() {
    fname="$1"
    file_ost_idx="$2"

    # get params
    parse_lfs_df  # now "lfsinfo_pct" and "lfsinfo_avail" is set
    occupancy_current=${lfsinfo_pct[OST:${file_ost_idx}]}
    occupancy_sum=${lfsinfo_pct[sum]}

    # skip if current OST occupancy is less than gross mean
    if [[ $occupancy_current -le $occupancy_sum ]] 
    then
        exit 1
    fi

    # main
    migrate_without_check "$fname" "$file_ost_idx"
}


main_find() {
    export -f \
        run_by_file_find_mode \
        parse_lfs_df \
        migrate_without_check \
        ost_idx_with_most_space_from_lfsinfo_avail \
        ost_idx_with_most_space_from_lfsinfo_pct

    find "${args[topdir]}" -type f -size +${args[minsize]} -uid +1000 | 
        xargs -d $'\n' -n 1 -P ${args[parallel]} bash -c 'run_by_file_find_mode "$@"' _
}


main_lfsfind() {
    export -f \
        run_by_file_lfsfind_mode \
        parse_lfs_df \
        migrate_without_check \
        ost_idx_with_most_space_from_lfsinfo_avail \
        ost_idx_with_most_space_from_lfsinfo_pct

    lfs find /home/users --ost "${args[ostidx]}" --size +${args[minsize]} |
        while read fname ; do
            uid="$(stat --format %u "$fname")"
            if [[ $uid -gt 1000 ]] ; then
                echo ${fname}
                echo ${args[ostidx]}
            fi
        done |
            xargs -d $'\n' -n 2 -P ${args[parallel]} bash -c 'run_by_file_lfsfind_mode "$@" || exit 255' _
}


# main
#run_by_file_old "${args[topdir]}" ${args[minsize]} "${args[tmpdir]}"
if [[ ${args[mode]} = lfs_find ]] ; then
    main_lfsfind
elif [[ ${args[mode]} = find ]] ; then
    main_find
fi

