#!/bin/bash
#================================================================
# btb-phylo
#================================================================
#%
#% DESCRIPTION
#%    Runs the full btb-phylo pipeline with a set of optional 
#%    input arguments
#%
#% INPUTS
#%  positional:
#%      results             path to results folder
#%      consensus           path to download folder for consensus 
#%                          files
#%  optional:
#%      --threads | -j      the number of threads to use for 
#%                          building snp-matrix
#%      --config | -c       path to a configuration file for 
#%                          filtering samples
#%      --meta_path | -m    path to folder containing cattle and 
#%                          movement csv files
#%      --with-docker       run inside docker container                 


SCRIPT="${0##*/}"

# parses named optional input args
# Thanks: https://stackoverflow.com/questions/71349910/optional-arguments-in-bash-script
parse_args() {
    THREADS=$(nproc)
    CONFIG=0
    DOCKER=0
    VIEWBOVINE=0
    SOPTS="j:c:m:"
    LOPTS="with-docker,threads:,config:,meta_path:"

    TMP=$(getopt -o "$SOPTS" -l "$LOPTS" -n "$SCRIPT" -- "$@") || exit 1

    eval set -- "$TMP"
    unset TMP

    while true; do
        case "$1" in
            -j | --threads ) THREADS=$2; shift;;
            -c | --config )  CONFIG=$2; shift;;
            -m | --meta_path ) VIEWBOVINE=1; CATTLE_AND_MOVEMENT=$2; shift;;
            --with-docker ) DOCKER=1;;
            -- ) shift; break;;
        esac
        shift
    done

    # required positional arguemnts
    if (( $#<2 ))
    then
        printf "Must include positional arguments of paths to: results folder "
        printf "and consensus folder\n"
        exit 1
    fi
    RESULTS=$1
    CONSENSUS=$2
}

# parse arguments
parse_args "$@"

btb_phylo_path="$(dirname $(realpath $0))/btb_phylo.py"

if [ $CONFIG == 0 ]; then
    CONFIG="$(dirname $(realpath $0))/filter.json" 
    echo "{}" > $CONFIG
fi

ALL_SAMPLES="$(dirname $(realpath $0))/all_wgs_samples.csv"

# if running with docker 
if [ $DOCKER == 1 ]; then
    printf "\nRunning btb-phylo with docker\n\n"
    if [ ! -f $ALL_SAMPLES ]
    then
        echo -e "Sample,GenomeCov,MeanDepth,NumRawReads,pcMapped,Outcome,flag,group,CSSTested,"\
            "matches,mismatches,noCoverage,anomalous,Ncount,ResultLoc,ID,TotalReads,Abundance,"\
            "Submission" > $ALL_SAMPLES
    fi
    if [ ! -d $RESULTS ]
    then
        mkdir $RESULTS
    fi
    # run docker container - this runs this script (btb-phylo.sh) inside the container (without --with-docker)
    if [ $VIEWBOVINE == 1 ]; then
	# pull the prod branch docker image from DockerHub 
	docker pull aphacsubot/btb-phylo:prod
        docker run --rm -it \
            --mount type=bind,source=$RESULTS,target=/results \
            --mount type=bind,source=$CONSENSUS,target=/consensus \
            --mount type=bind,source=$CONFIG,target=/config.json \
            --mount type=bind,source=$ALL_SAMPLES,target=/btb-phylo/all_samples.csv \
            --mount type=bind,source=$CATTLE_AND_MOVEMENT,target=/btb-phylo/cattle_and_movement \
            aphacsubot/btb-phylo:prod /results /consensus -c /config.json -j $THREADS -m cattle_and_movement 
    else
	# pull the current branch docker image from DockerHub 
	branch=$(git rev-parse --abbrev-ref HEAD)
	docker pull aphacsubot/btb-phylo:$branch
        docker run --rm -it \
            --mount type=bind,source=$RESULTS,target=/results \
            --mount type=bind,source=$CONSENSUS,target=/consensus \
            --mount type=bind,source=$CONFIG,target=/config.json \
            --mount type=bind,source=$ALL_SAMPLES,target=/btb-phylo/all_samples.csv \
            aphacsubot/btb-phylo:$branch /results /consensus -c /config.json -j $THREADS
    fi
# if not running with docker (or running inside the docker container)
else
    # run the pipeline
    if [ $VIEWBOVINE == 1 ]; then
        python $btb_phylo_path ViewBovine $RESULTS $CONSENSUS $CATTLE_AND_MOVEMENT
    else
        python $btb_phylo_path full_pipeline $RESULTS $CONSENSUS -j $THREADS --config $CONFIG
    fi
fi
