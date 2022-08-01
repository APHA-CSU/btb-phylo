#!/bin/bash
#================================================================
# btb-phylo
#================================================================

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

    echo $@

    while true; do
        case "$1" in
            -j | --threads ) THREADS=$2; shift;;
            -c | --config )  CONFIG=$2; shift;;
            -m | --meta_path ) VIEWBOVINE=1; CATTLE_AND_MOVEMENT=$2; shift;;
            --) shift; break;;
        esac
        shift
    done

    if (( $#<2 ))
    then
        printf "Must include positional arguments of paths to: results folder "
        printf "and consensus folder\n"
        exit 1
    fi

    RESULTS=$1
    CONSENSUS=$2
    shift 2

    nargs=$#
    for ((i=0; i<nargs; ++i)); do
        if [ $1 == "with-docker" ]
        then
        DOCKER=1
        else
        printf "Unrecognised argument: '%s'. Please include positional arguments for " "$1"
        printf "paths to: results folder and consensus folder. Additional optional arguments "
        printf "include 'with-docker', '-vb', '-j' and '-c' (see Readme)\n"
        exit 1
        fi
        shift
    done
}

# parse arguments
parse_args "$@"

if [ $CONFIG == 0 ]; then
    echo "{}" > filter.json
    CONFIG=$(realpath filter.json)
fi

if [ $DOCKER == 1 ]; then
    printf "\nRunning btb-phylo with docker\n\n"
    if [ ! -f all_samples.csv ]
    then
        echo -e "Sample,GenomeCov,MeanDepth,NumRawReads,pcMapped,Outcome,flag,group,CSSTested,matches,mismatches,noCoverage,anomalous,Ncount,ResultLoc,ID,TotalReads,Abundance,Submission" > all_samples.csv
    fi
    ALL_SAMPLES=$(realpath all_samples.csv)
    docker pull aphacsubot/btb-phylo:consistify
    if [ $VIEWBOVINE == 1 ]; then
        docker run --rm -it --mount type=bind,source=$RESULTS,target=/results --mount type=bind,source=$CONSENSUS,target=/consensus --mount type=bind,source=$CONFIG,target=/config.json --mount type=bind,source=$ALL_SAMPLES,target=/btb-phylo/all_samples.csv aphacsubot/btb-phylo:dockerize /results /consensus -c /config.json -j $THREADS -m $CATTLE_AND_MOVEMENT 
    else
        docker run --rm -it --mount type=bind,source=$RESULTS,target=/results --mount type=bind,source=$CONSENSUS,target=/consensus --mount type=bind,source=$CONFIG,target=/config.json --mount type=bind,source=$ALL_SAMPLES,target=/btb-phylo/all_samples.csv aphacsubot/btb-phylo:dockerize /results /consensus -c /config.json -j $THREADS
    fi
else
    if [ $VIEWBOVINE == 1 ]; then
        python btb_phylo.py full_pipeline $RESULTS $CONSENSUS -j $THREADS --config $CONFIG --viewbovine --meta_path $CATTLE_AND_MOVEMENT
    else
        python btb_phylo.py full_pipeline $RESULTS $CONSENSUS -j $THREADS --config $CONFIG
    fi
fi