import tempfile
import os
import pandas as pd

import utils

DEFAULT_RESULTS_BUCKET = "s3-csu-003"
DEFAULT_SUMMARY_KEY = "v3/summary/test.csv"

class InvalidDtype(Exception):
    def __init__(self, message="Invalid series name. Series must be of correct type", 
                 *args, **kwargs):
        super().__init__(message, args, kwargs)
        if "dtype" in kwargs:
            self.message = f"Invalid series name. Series must be of type {kwargs['dtype']}"
        else:
            self.message = message

    def __str__(self):
        return self.message


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
        summary_df = pd.read_csv(summary_filepath, dtype = {"sample_name":object,
                                 "submission":object, "project_code":"category",
                                 "sequencer":"category", "run_id":object,
                                 "well":"category", "read_1":object, "read_2":object, 
                                 "lane":"category", "batch_id":"category", 
                                 "reads_bucket":"category", "results_bucket":"category", 
                                 "results_prefix":object, "sequenced_datetime":object, 
                                 "GenomeCov":float, "MeanDepth":float, "NumRawReads":int, 
                                 "pcMapped":float, "Outcome":"category", "flag":"category",
                                 "group":"category", "CSSTested":float, "matches":float,
                                 "mismatches":float, "noCoverage":float, "anomalous":float}
                                )
      #      batches = []
      #      for project_code in PROJECT_CODES:
      #          batches.extend(utils.list_s3_objects(bucket, os.path.join("nickpestell/v3", project_code, "")))
      #      for batch_prefix in batches:
      #          s3_uris = [(bucket, key) for key in utils.list_s3_objects(bucket, os.path.join(batch_prefix, ""))]
      #          append_multi_fasta(s3_uris, multi_fasta_path)
      #  else:
      #      s3_uris = [(bucket, key) for key in keys]
      #      append_multi_fasta(s3_uris, multi_fasta_path)
    return summary_df

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
    summary_df = filter_df_categorical(summary_df, "Outcome", ["Pass"])
    summary_df = filter_df_numeric(summary_df, "pcMapped", pcmap_threshold)
    for column, values in kwargs.items():
        try:
            if not (pd.api.types.is_categorical_dtype(summary_df[column]) or\
                    pd.api.types.is_object_dtype(summary_df[column])):
                summary_df = filter_df_categorical(summary_df, column, values)
            elif pd.api.types.is_numeric_dtype(summary_df[column]):
                summary_df = filter_df_numeric(summary_df, column, values)
        except KeyError as e:
            raise ValueError(f"Inavlid kwarg '{column}': must be one of: " 
                             f"{summary_df.columns.to_list()}")
    if summary_df.empty:
        raise Exception("0 samples meet specified criteria")
    return summary_df
    
def filter_df_numeric(df, column_name, values):
    """ 
        Filters the summary dataframe according to the values in 
        'column_name' with min and max values defined by elements in
        'values'. The data in column name must me of dtype int or float
        and the argument 'values' must be of type list or tuple and 
        of length 2. 
    """ 
    if not pd.api.types.is_numeric_dtype(df[column_name]):
        raise InvalidDtype(dtype="float or int")
    if len(values) != 2:
        raise ValueError("pcmap_threshold must be of length 2")
    df_filtered = df.loc[(df[column_name] > values[0]) & (df[column_name] < values[1])]
    df_filtered.reset_index(inplace=True, drop=True)
    return df_filtered

def filter_df_categorical(df, column_name, values):
    """ 
        Filters the summary dataframe according to the values in 
        'column_name' retaining all rows which have summary_df[column_name]
        matching one on the elements in the input list 'values'. 
    """ 
    if not (pd.api.types.is_categorical_dtype(df[column_name]) or \
            pd.api.types.is_object_dtype(df[column_name])):
        raise InvalidDtype(dtype="category or object")
    if not isinstance(values, list):
        raise ValueError("Invalid kwarg value: must be of type list")
    df_filtered = df.loc[df[column_name].isin(values)]
    df_filtered.reset_index(inplace=True, drop=True)
    return df_filtered

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
    """
        Run snp-sites on consensus files, then runs snp-dists on the results
    """

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
