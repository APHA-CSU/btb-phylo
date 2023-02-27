#!/bin/bash
set -e

META_PATH=$2

# backup production files
# skip the backup stage if the prod/ folder is missing in s3-ranch-042
{
    # extract date of current production files from s3-ranch-042
    aws s3 cp s3://s3-ranch-042/prod/metadata/metadata.json . &&
    {
	backup_datetime=($(sed -e 's/^"//' -e 's/"$//' <<< $(jq '.datetime' metadata.json)))
	backup_date=${backup_datetime[0]}
	rm metadata.json

	# backup current production files in s3-ranch-042
	aws s3 mv --recursive s3://s3-ranch-042/prod/ s3://s3-ranch-042/${backup_date}/ --acl bucket-owner-full-control
    }
}

# delete production files if still existing
aws s3 rm --recursive s3://s3-ranch-042/prod/

# run btb-phylo 
consensus_path=.ViewBovine_consensus
mkdir -p ${consensus_path}
./btb-phylo.sh .ViewBovine_results ${consensus_path} --meta_path ${META_PATH} --with-docker

# upload new production files to s3-ranch-042
aws s3 cp --recursive .ViewBovine_results/ s3://s3-ranch-042/prod/ --acl bucket-owner-full-control
