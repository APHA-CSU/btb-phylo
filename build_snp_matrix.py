import tempfile
import os
import pandas as pd

import utils

DEFAULT_RESULTS_BUCKET = "s3-csu-003"
DEFAULT_SUMMARY_KEY = "v3/summary/test.csv"

def build_multi_fasta(multi_fasta_path, bucket=DEFAULT_RESULTS_BUCKET,
                      summary_key=DEFAULT_SUMMARY_KEY, keys=None, 
                      pcmap_threshold=(0,100), **kwargs):
    """
    # keys is a list of s3_uris - for bespoke requests
    # if None download on plate-by-plate basis
    # TODO: ensure there are no duplicates - need policy for choosing the correct sample, probably based on pc-mapped
    # TODO: make filenames consistent - many have "consensus.fas" at the end
    # based on highes pc-mapped.
    """
      #  if keys == None:
            # Download btb_wgs_samples.csv
    with tempfile.TemporaryDirectory() as temp_dirname:
        summary_filepath = os.path.join(temp_dirname, "samples.csv")
        utils.s3_download_file(bucket, summary_key, summary_filepath)
        summary_df = pd.read_csv(summary_filepath)
      #      batches = []
      #      for project_code in PROJECT_CODES:
      #          batches.extend(utils.list_s3_objects(bucket, os.path.join("nickpestell/v3", project_code, "")))
      #      for batch_prefix in batches:
      #          s3_uris = [(bucket, key) for key in utils.list_s3_objects(bucket, os.path.join(batch_prefix, ""))]
      #          append_multi_fasta(s3_uris, multi_fasta_path)
      #  else:
      #      s3_uris = [(bucket, key) for key in keys]
      #      append_multi_fasta(s3_uris, multi_fasta_path)

def filter_samples(summary_df, pcmap_threshold=(0,100), **kwargs):
    """ Filters the sample summary dataframe which is based off 
        btb_wgs_samples.csv according to a set of criteria. 

        Parameters:
            summary_df (pandas dataframe object): a dataframe read from 
            the btb_wgs_samples.csv.

            pcmap_threshold (tuple): min and max thresholds for pcMapped

            **kwargs (list): 0 or more optional arguments. Names must match
            a column name in btb_wgs_samples.csv. Vales must be of type 
            list, specifying a set of values to match against the argument 
            name's column in btb_wgs_samples.csv. For example, 
            'sample_name=["AFT-61-03769-21", "20-0620719"]' will include 
            just these two samples.

        Returns:
            summary_df (pandas dataframe object): a dataframe of 'Pass'
            only samples filtered according to criteria set out in 
            arguments.
    """
    if len(pcmap_threshold) != 2:
        raise ValueError("pcmap_threshold must be of length 2")
    if not isinstance(pcmap_threshold[0], int, float) and \
        not isinstance(pcmap_threshold[0], int, float):
        raise ValueError("pcmap_threshold elements must be numeric") 
    for column, values in kwargs.items():
        if column not in summary_df.columns:
            raise ValueError("Invalid kwarg: must be a column name from"
                            "btb_wgs_samples.csv")
        if not isinstance(values, list):
            raise ValueError("Invalid kwarg value: must be of type list")
        summary_df = summary_df.loc[summary_df[column].isin(values)]
    summary_df = summary_df.loc[summary_df["Outcome"]=="Pass"]
    summary_df = summary_df.loc[(summary_df["pcMapped"] > pcmap_threshold[0])| 
                                (summary_df["pcMapped"] > pcmap_threshold[1])]
    if summary_df.empty:
        raise Exception("0 samples meet specified criteria")
    return summary_df

def append_multi_fasta(s3_uris, multi_fasta_path):
    '''
    Builds a file appended with consensus sequences of samples for phylogeny
    
    Parameters:
        s3_uris (list of tuples): list of s3 bucket and key pairs in tuple
        multi_fasta_path (string): local filepath for output file
    '''
    with open(multi_fasta_path, 'wb') as out_file:
        for s3_uri in s3_uris:
            with tempfile.TemporaryDirectory() as temp_dirname:
                consensus_filepath = os.path.join(temp_dirname, "temp.fas") 
                utils.s3_download_file(s3_uri[0], s3_uri[1], consensus_filepath)
                with open(consensus_filepath, 'rb') as consensus_file:
                    out_file.write(consensus_file.read())

def snps(output_path, multi_fasta_path):
    """run snp-sites on consensus files, then runs snp-dists on the results"""

    # run snp sites 
    cmd = f'snp-sites {multi_fasta_path} -c -o {output_path}snpsites.fas'
    utils.run(cmd, shell=True)

    # run snp-dists
    cmd = f'snp-dists {output_path}snpsites.fas > {output_path}snps.tab'
    utils.run(cmd, shell=True)

def main():
    # parse batches.csv - maybe not, becasue this data is for raw reads - it maybe implicit in s3-csu-003.
    # parse outcomes.csv ->  this is called something like 20079_FinalOut_18Oct21.csv, for determining if samples should be included: output from btb-seq (cam)
    # build multi-fasta
    # run snp-sites
    # run snp-dist
    s3_uris = [("s3-staging-area", "nickpestell/#"), ("s3-staging-area", "nickpestell/AF-21-05371-19_consensus.fas")]
    keys = ["nickpestell/#", "nickpestell/AF-21-05371-19_consensus.fas"]
    with tempfile.TemporaryDirectory() as temp_dirname:
        multi_fasta_path = os.path.join(temp_dirname, "multi-fasta.fas")
        build_multi_fasta(multi_fasta_path, "s3-staging-area")#, keys)
        snps("/home/nickpestell/tmp/", multi_fasta_path)

if __name__ == "__main__":
    main()
