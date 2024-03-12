#!/bin/bash
# modification log
    # 220318
        # added --topdir option
        # file size in GB is printed 
        # file last access time is printed
        # modified awk script

    # 220321
        # added --depth option
        # added get_search_target_list() function
    # 220330
        # quoted "search_target" parameter
        # quoted find results in the function "get_search_target_list"

    # 220412
        # added timestamps
        # PARALLEL constant is set as the conda_wrapper script
        # process is also written to genlog file

    # 230116
        # added -v skippat="/[^/]*conda[^/]*/" to awk in "write_script_with_file" function
            # skips anaconda or miniconda directory

    # 230117
        # revised get_search_target_list function
        # revised argparse (using "args" parameter)

set -eu


PARALLEL=/home/users/pjh/scripts/conda_wrapper/parallel
BASH=/usr/bin/bash


function usage {
    echo "Recursively examines all files under <topdir>."
    echo 
    echo "Arguments:"
    echo -e "\t--outdir: Directory where results are saved. Creates a new directory if not already present."
    echo -e "\t--topdir: Top directory from where to begin search. Default: ${defaults[topdir]}"
    echo -e "\t-p: Number of parallelization. Default: ${defaults[p]}"
    echo -e "\t--mtime: -mtime option value. Default: ${defaults[mtime]}"
    echo -e "\t--size: -size option value. Default: ${defaults[size]}"
    echo -e "\t--depth: Result files are created for each N-depth files under <topdir>. Must be a non-negative integer. Default: ${defaults[depth]}.\n\t\tWhen depth is 0, only one result file named <topdir>.size_\${size}GB_mtime_\${mtime}_summary will be created.\n\t\tWhen depth is 1, result files will be created for each output of 'find <topdir> -mindepth 1 -maxdepth 1'."
    echo -e "\t--merge: If set, all result files are concatenated into a single file named <topdir>.size_\${size}GB_mtime_\${mtime}_summary"
    exit 1
}


function rootwarn {
    if ! [[ $USER = root ]] ; then
        echo "You are not a root user."
        echo "Result will be incomplete since you cannot access some of other user's files."
        echo "Enter \"yes\" if you want to proceed."
        read input
        if ! [[ $input = yes ]] ; then exit ; fi
    fi
}


function argparse {
    # set params
    declare -gA defaults
    defaults[topdir]=/home/users
    defaults[p]="10"
    defaults[mtime]="+10"
    defaults[size]="+1G"
    defaults[depth]="1"
    defaults[merge]=false

    required_args=(
        outdir
    )

    # main
    declare -gA args
    if [[ $# = 0 ]] ; then
        usage
    else
        while [[ $# -gt 0 ]] ; do
            case "$1" in
                --outdir)
                    shift ; args[outdir]="$1" ; shift ;;
                --topdir)
                    shift ; args[topdir]="$1" ; shift ;;
                -p)
                    shift ; args[p]="$1" ; shift ;;
                --mtime)
                    shift ; args[mtime]="$1" ; shift ;;
                --size)
                    shift ; args[size]="$1" ; shift ;;
                --depth)
                    shift ; args[depth]="$1" ; shift ;;
                --merge)
                    args[merge]=true ; shift ;;
                -h|--help|*)
                    usage ;;
            esac
        done
    fi

    # setting default args
    for key in ${!defaults[@]} ; do
        args[$key]=${args[$key]:-${defaults[$key]}}
    done

    # check if required arguments are all set
    for key in ${required_args[@]:-} ; do
        if [[ -z ${args[$key]:-} ]] ; then
            echo "Required argument --${key} is not set."
            exit 1
        fi
    done
    
    # sanity check
    if [[ ! -d ${args[topdir]} ]] ; then
        echo "Error: --topdir is not a directory."
        exit 1
    elif [[ -e ${args[outdir]} ]] ; then
        if [[ !( -d ${args[outdir]} && $(ls ${args[outdir]} | wc -l) = 0 ) ]] ; then
            echo "Error: If --outdir already exists, it must be an empty directory."
            exit 1
        fi
    fi
}


function normalize_path {
    awk '{
        gsub("/{2,}", "/", $0)
        gsub("/$", "", $0)
        print($0)
    }' <<< "$1"
}


function get_depth {
    top="$1"
    current="$2"

    top="$( normalize_path "$top" )"
    current="$( normalize_path "$current" )"

    awk -v pat="^${top}" '{
        gsub(pat, "", $0)
        if ($0 == "") {
            print(0)
        } else { 
            print(split($0, arr, "/") - 1) 
        } 
    }' <<< "$current"
}


function get_search_target_list {
    search_target_list=()
    while read fname ; do
        depth=$(get_depth "${args[topdir]}" "$fname")
        if [[ $depth = ${args[depth]} ]] ; then
            search_target_list+=( "$fname" )
        elif [[ $depth -lt ${args[depth]} ]] ; then
            if [[ -f "$fname" ]] ; then
                search_target_list+=( "$fname" )
            fi
        else
            echo "Error: 'find' prints depth greater than --depth argument: $fname"
            exit 1
        fi
    done < <( find ${args[topdir]} -maxdepth ${args[depth]} \( -type f -o -type d \) )
}


function get_search_target_list_old {
    search_target_list=()

    for i in $(seq 0 ${args[depth]}) ; do
        if [[ $i = ${args[depth]} ]] ; then
            while read fname ; do
                search_target_list+=( "$fname" )
            done < <( find ${args[topdir]} -mindepth $i -maxdepth $i )
        else
            while read fname ; do
                search_target_list+=( "$fname" )
            done < <( find ${args[topdir]} -mindepth $i -maxdepth $i -type f )
        fi
    done
}


function make_outdirs {
    mkdir -p ${args[outdir]}

    script_dir=${args[outdir]}/scripts
    mkdir -p $script_dir

    log_dir=${args[outdir]}/logs
    mkdir -p $log_dir

    tmp_dir=${args[outdir]}/tmpfiles
    mkdir -p $tmp_dir

    result_dir=${args[outdir]}/results
    mkdir -p $result_dir

    args_log=${args[outdir]}/args
    declare -p args > "$args_log"
}


function make_scripts {
    outfile_list=()

    for search_target in "${search_target_list[@]}" ; do # search_target can be a non-directory file.
        #pf="${search_target// /_}"
        #pf="${pf////_____}"

        pf="${search_target////________}"
        outfile="${tmp_dir}/${pf}.size_${args[size]}_mtime_${args[mtime]}_summary"
        outfile_list+=( "$outfile" )

        findlog="${log_dir}/${pf}.log"
        awklog="${log_dir}/${pf}.awklog"

        script="${script_dir}/${pf}_run.sh"

        cat > "$script" <<-EOF
			#!${BASH}
			
			search_target="$search_target"
			outfile="$outfile"
			findlog="$findlog"
			awklog="$awklog"
			mtime="${args[mtime]}"
			size="${args[size]}"
			
			echo "[\$(date)] BEGINNING search for \"\$search_target\""
			
			EOF

        write_script_with_file "$script"

        cat >> "$script" <<-EOF
			
			echo "[\$(date)] FINISHED search for \$search_target"
			EOF
    done
}


function write_script_with_file {
    cat >> "$1" <<-EOF
		find "\$search_target" -type f -size \${size} -mtime \${mtime} -printf '%p\t%s\t%u\t%Ac\t%A@\t%Tc\t%T@\t' -exec file -bi {} \\; 2> "\$findlog" | 
		    awk -v skippat="/[^/]*conda[^/]*/" '
		    function gb(num) {
		        return sprintf("%.3f", num / (1024)^3)
		    }
		    BEGIN { 
		        OFS = "\t"
		        FS = "\t" 
		        print "filename", "filesize", "filesize_GB", "username", "lastaccess", "lastaccess_sec", "lastmodify", "lastmodify_sec", "filetype", "encoding"
		    }
		    { 
		        filename = \$1
		        filesize = \$2
		        filesize_GB = gb(filesize)"GB"
		        username = \$3
		        lastaccess = \$4
		        lastaccess_sec = \$5
		        lastmodify = \$6
		        lastmodify_sec = \$7

                if (filename ~ skippat) {
                    next
                }

                split(\$NF, arr1, "; ")
                split(arr1[2], arr2, "=")
		        filetype = arr1[1]
		        encoding = arr2[2]
		
		        print filename, filesize, filesize_GB, username, lastaccess, lastaccess_sec, lastmodify, lastmodify_sec, filetype, encoding
		    }' 1> "\$outfile" 2> "\$awklog"
		EOF
}


# NOT USED
function write_script_without_file {
    cat >> $1 <<-EOF
		find "\$search_target" -type f -size \${size} -mtime \${mtime} -printf '%p\t%s\t%u\t%Ac\n' 2> \$findlog | 
		    awk '
		    function gb(num) {
		        return sprintf("%.3f", num / (1024)^3)
		    }
		    BEGIN { 
		        OFS = "\\t"
		        FS = "\\t" 
		        print "filename", "filesize", "filesize_GB", "username", "lastaccess"
		    }
		    { 
		        filename = \$1
		        filesize = \$2
		        filesize_GB = gb(filesize)"GB"
		        username = \$3
		        lastaccess = \$4
		
		        print filename, filesize, filesize_GB, username, lastaccess
		    }' 1> \$outfile 2> \$awklog
		EOF
}


function run {
    genlog=${args[outdir]}/genlog
    find ${script_dir} -type f | 
        sort | 
        $PARALLEL --line-buffer -j ${args[p]} $BASH |&
        tee $genlog
}


function merge_results {
    #if [[ $merge = 1 ]] ; then
    if ${args[merge]} ; then
        merged_result="${result_dir}/$(basename ${args[topdir]}).size_${args[size]}_mtime_${args[mtime]}_summary"

        head -n1 "${outfile_list[0]}" > "$merged_result"
        for fname in "${outfile_list[@]}" ; do
            tail -n +2 "$fname" >> "$merged_result"
        done
    else
        mv "${outfile_list[@]}" "$result_dir"
    fi

    rm -r "$tmp_dir"
}


# main
argparse "$@"
rootwarn
get_search_target_list
make_outdirs
make_scripts
run
merge_results
