#!/usr/bin/bash
#init: 211019(?)

set -eu

function usage {
    echo -e "Description:"
    echo -e "\tApplies some modification to a newly installed R-containing conda environment which is required to run well with Rstudio."
    echo -e
    echo -e "Usage: $(basename $0) \\"
    echo -e "\t--condapf: Conda environment directory, which looks like \"*/miniconda3/envs/<envname>\""
    exit 1
}


function argparse {
    declare -gA defaults

    required_args=(
        condapf
    )

    # do argument parsing
    declare -gA args
    while [[ $# -gt 0 ]] ; do
        case "$1" in
            --condapf)
                shift ; args[condapf]="$1" ; shift ;;
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
}


function set_parameters
{
	conda_R_etc=${args[condapf]}/lib/R/etc
	conda_bin=${args[condapf]}/bin
	Rscript=${conda_bin}/Rscript
	R=${conda_bin}/R
	#__cran_unist="https://cran.biodisk.org/"
}


function make_Renviron_site
{
    # Sets RETICULATE_PYTHON environment variable ; only relevant when using reticulate

    reticulate_env_python=$(dirname ${args[condapf]})/r-reticulate/bin/python
    if [[ -e ${reticulate_env_python} ]] ; then
        echo "RETICULATE_PYTHON=${reticulate_env_python}" >> ${conda_R_etc}/Renviron.site
    fi
}


function make_Rprofile_site
{
    # Makes a user-specific R library installation directory under user's home directory 
    # Uses it using .libPaths function

    custom_rprofile_site=$(dirname $0)/Rprofile.site
    cp $custom_rprofile_site $conda_R_etc
}


function edit_Makeconf
{
    # This forces R to use C libraries within the conda environment when installing a new R package.
    # Without this, R uses system C libraries which frequently results in compilation failure

    makeconf_original=${conda_R_etc}/Makeconf
    makeconf_backup=${conda_R_etc}/Makeconf.backup
    makeconf_tmp=${conda_R_etc}/Makeconf.tmp

	cp ${makeconf_original} ${makeconf_backup}
	awk \
		-v pat="([[:blank:]])(x86[^[:blank:]]+)" \
		-v bin="${conda_bin}" \
		'{print gensub(pat, "\\1" bin "/" "\\2", "g", $0)}' $makeconf_original \
        > $makeconf_tmp
	mv $makeconf_tmp $makeconf_original
    chmod --reference $makeconf_backup $makeconf_original
}


function install_packages
{
	### PACKAGES NEEDED TO BE INSTALLED WITHOUT CONDA ###
	
	# hash (required for copynumbersypark)
	$Rscript -e "install.packages(\"hash\", repos = \"${__cran_unist}\")" 
	
	# sequenza julab
	$R CMD INSTALL ~pjh/tools/R/sequenza_source/copynumbersypark
	$R CMD INSTALL ~pjh/tools/R/sequenza_source/sequenza.v2.julab
	$R CMD INSTALL ~pjh/tools/R/sequenza_source/sequenza.v3.julab
	
	# update packages (required for installing DUBStepR, RCAv2, and SCopeLoomR)
	$Rscript -e "devtools::update_packages(upgrade = \"always\")"
	
	# DUBStepR
	$Rscript -e "devtools::install_github(\"prabhakarlab/DUBStepR\")" 
	
	# RCAv2
	$Rscript -e "remotes::install_github(\"prabhakarlab/RCAv2\")"
	
	# SCopeLoomR (conda version is too old)
	$Rscript -e "devtools::install_github(\"aertslab/SCopeLoomR\", build_vignettes = TRUE)"
	# SCENIC
	$Rscript -e "devtools::install_github(\"aertslab/SCENIC\")"
}


function main {
    set_parameters
    make_Renviron_site
    make_Rprofile_site
    edit_Makeconf
    #install_packages
}


# main
argparse "$@"
main


### MAIN ###

#trap 'echo Error occured. Aborting. ; trap ERR ; return' ERR

#if [[ -z $CONDA_PREFIX ]] ; then
#    echo "CONDA_PREFIX is not set!"
#else
#    echo "CONDA_PREFIX is : $CONDA_PREFIX"
#    read -p "Enter y if you want to proceed " __input__
#
#    if [[ $__input__ = y ]] ; then
#        set_parameters
#        _make_Renviron_site_
#        _make_Rprofile_site_
#        _edit_Makeconf_
#        #_install_packages_
#    fi
#
#    echo ALL FINISHED
#fi

#unset __input__ _set_parameters_ _make_Renviron_site_ _make_Rprofile_site_ _edit_Makeconf_ _install_packages_
#trap ERR
