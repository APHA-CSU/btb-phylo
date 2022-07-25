#!/bin/bash
#================================================================
# btb-phylo
#================================================================

SCRIPT="${0##*/}"

parse_args() {
    THREADS=$(nproc)
    DOCKER=0
    CONFIG=0
    SOPTS="j:c:"

    TMP=$(getopt -o "$SOPTS" -n "$SCRIPT" -- "$@") || exit 1

    eval set -- "$TMP"
    unset TMP

    while true; do
        case "$1" in
            -j)
                THREADS=$2
                shift
                ;;
            -c)
                CONFIG=$2
                shift
                ;;
            --)
                shift
                break
                ;;
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
        printf "include 'with-docker', '-j' and '-c' (see Readme)\n"
        exit 1
        fi
        shift
    done
}

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
    docker pull aphacsubot/btb-phylo:dockerize
    docker run --rm -it --mount type=bind,source=$RESULTS,target=/results --mount type=bind,source=$CONSENSUS,target=/consensus --mount type=bind,source=$CONFIG,target=/config.json --mount type=bind,source=$ALL_SAMPLES,target=/btb-phylo/all_samples.csv aphacsubot/btb-phylo:dockerize /results /consensus -c /config.json -j $THREADS
else
    python btb_phylo.py full_pipeline $RESULTS $CONSENSUS -j $THREADS --config $CONFIG 
fi