#!/bin/bash
set -eu

parse_lfs_df() {
    declare -gA lfsinfo
    while read line ; do
        read -a linesp <<< "$line"

        if [[ (${linesp[0]:-} = "UUID") || -z ${linesp[0]:-} ]]
        then 
            continue
        fi

        pct=$(tr -d '%' <<< ${linesp[4]})

        if [[ ${linesp[5]} = "/home/users" ]]
        then
            key="sum"
        else
            key=$(awk '{print gensub("^.*\\[(.+)\\]$", "\\1", "g", $0)}' <<< ${linesp[5]})
        fi

        lfsinfo[$key]=$pct
    done < <(lfs df)
}


get_least_ost() {
    for key in ${!lfsinfo[@]} ; do
        if [[ $key = "sum" || $key =~ "MDT" ]] ; then
            continue
        fi
        echo $key ${lfsinfo[$key]}
    done | sort -k2,2n | head -n1 | awk '{print gensub("OST:", "", "g", $1)}'
}


parse_lfs_df
get_least_ost
