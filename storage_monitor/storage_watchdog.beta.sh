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

set -eu


PARALLEL=/home/users/pjh/scripts/conda_wrapper/parallel
BASH=/usr/bin/bash


function usage {
    echo "Recursively examines all files under <topdir>."
    echo 
    echo "Arguments:"
    echo -e "\t-o|--outdir: Directory where results are saved. Creates a new directory if not already present."
    echo -e "\t--topdir: Top directory from where to begin search. Default: /home/users"
    echo -e "\t-p: Number of parallelization. Default = 10"
    echo -e "\t--mtime: -mtime option value. Default = +10"
    echo -e "\t--size: -size option value. Default = +1G"
    echo -e "\t--depth: Result files are created for each N-depth files under <topdir>. Must be a non-negative integer. Default = 1.\n\t\tWhen depth is 0, only one result file named <topdir>.size_\${size}GB_mtime_\${mtime}_summary will be created.\n\t\tWhen depth is 1, result files will be created for each output of 'find <topdir> -mindepth 1 -maxdepth 1'."
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
    if [[ $# = 0 ]] ; then
        usage
    else
        while [[ $# -gt 0 ]] ; do
            case "$1" in
                -o|--outdir)
                    shift ; outdir="$1" ; shift ;;
                --topdir)
                    shift ; topdir="$1" ; shift ;;
                -p)
                    shift ; nt="$1" ; shift ;;
                --mtime)
                    shift ; mtime="$1" ; shift ;;
                --size)
                    shift ; size="$1" ; shift ;;
                --depth)
                    shift ; depth="$1" ; shift ;;
                --merge)
                    merge=1 ; shift ;;
                -h|--help|*)
                    usage ;;
            esac
        done
    fi

    # setting default args
    if [[ -z ${topdir:-} ]] ; then topdir="/home/users" ; fi
    if [[ -z ${nt:-} ]] ; then nt=10 ; fi
    if [[ -z ${mtime:-} ]] ; then mtime=+10 ; fi
    if [[ -z ${size:-} ]] ; then size=+1G ; fi
    if [[ -z ${depth:-} ]] ; then depth=1 ; fi
    if [[ -z ${merge:-} ]] ; then merge=0 ; fi
    
    # sanity check
    if [[ ! -d $topdir ]] ; then
        echo "Error: --topdir is not a directory."
        exit 1
    elif [[ -e $outdir ]] ; then
        if [[ !( -d $outdir && $(ls $outdir | wc -l) = 0 ) ]] ; then
            echo "Error: If --outdir already exists, it must be an empty directory."
            exit 1
        fi
    fi
}


function get_search_target_list {
    search_target_list=()

    for i in $(seq 0 $depth) ; do
        if [[ $i = $depth ]] ; then
            while read fname ; do
                search_target_list+=( "$fname" )
            done < <( find $topdir -mindepth $i -maxdepth $i )
        else
            while read fname ; do
                search_target_list+=( "$fname" )
            done < <( find $topdir -mindepth $i -maxdepth $i -type f )
        fi
    done
}


function make_outdirs {
    mkdir -p $outdir

    script_dir=${outdir}/scripts
    mkdir -p $script_dir

    log_dir=${outdir}/logs
    mkdir -p $log_dir

    tmp_dir=${outdir}/tmpfiles
    mkdir -p $tmp_dir

    result_dir=${outdir}/results
    mkdir -p $result_dir
}


function make_scripts {
    outfile_list=()

    for search_target in "${search_target_list[@]}" ; do # search_target can be a non-directory file.
        pf=${search_target// /_}
        pf=${pf////_____}
        outfile=${tmp_dir}/${pf}.size_${size}_mtime_${mtime}_summary
        outfile_list+=( "$outfile" )

        findlog=${log_dir}/${pf}.log
        awklog=${log_dir}/${pf}.awklog

        script=${script_dir}/${pf}_run.sh

        cat > $script <<-EOF
			#!${BASH}
			
			search_target="$search_target"
			outfile=$outfile
			findlog=$findlog
			awklog=$awklog
			mtime="$mtime"
			size="$size"
			
			echo "[\$(date)] BEGINNING search for \$search_target"
			
			EOF

        write_script_with_file $script

        cat >> $script <<-EOF
			
			echo "[\$(date)] FINISHED search for \$search_target"
			EOF
    done
}


function write_script_with_file {
    cat >> $1 <<-EOF
		find "\$search_target" -type f -size \${size} -mtime \${mtime} -printf '%p\t%s\t%u\t%Ac\t%A@\t%Tc\t%T@\t' -exec file -bi {} \\; 2> \$findlog | 
		    awk '
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

                split(\$NF, arr1, "; ")
                split(arr1[2], arr2, "=")
		        filetype = arr1[1]
		        encoding = arr2[2]
		
		        print filename, filesize, filesize_GB, username, lastaccess, lastaccess_sec, lastmodify, lastmodify_sec, filetype, encoding
		    }' 1> \$outfile 2> \$awklog
		EOF
}


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
    genlog=${outdir}/genlog
    find ${script_dir} -type f | 
        sort | 
        $PARALLEL --line-buffer -j $nt $BASH |&
        tee $genlog
}


function merge_results {
    if [[ $merge = 1 ]] ; then
        merged_result=${result_dir}/$(basename ${topdir}).size_${size}_mtime_${mtime}_summary

        head -n1 ${outfile_list[0]} > $merged_result
        for fname in ${outfile_list[@]} ; do
            tail -n +2 $fname >> $merged_result
        done
    else
        mv ${outfile_list[@]} $result_dir
    fi

    rm -r $tmp_dir
}


# main
argparse "$@"
rootwarn
get_search_target_list
make_outdirs
make_scripts
run
merge_results
