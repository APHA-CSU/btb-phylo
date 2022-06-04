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
        if "column_name" in kwargs:
            self.message = f"Invalid series name '{kwargs['column_name']}'. Series must be of the correct type"
        if "column_name" in kwargs and "dtype" in kwargs:
            self.message = f"Invalid series name '{kwargs['column_name']}' Series must be of type {kwargs['dtype']}"
        else:
            self.message = message

    def __str__(self):
        return self.message

def summary_csv_to_df(bucket, summary_key):
    """
        Downloads btb_wgs_samples.csv and returns the data in a pandas dataframe.
    """
    with tempfile.TemporaryDirectory() as temp_dirname:
        summary_filepath = os.path.join(temp_dirname, "samples.csv")
        utils.s3_download_file(bucket, summary_key, summary_filepath)
        df = pd.read_csv(summary_filepath, dtype = {"sample_name":object,
                         "submission":object, "project_code":"category",
                         "sequencer":"category", "run_id":object,
                         "well":"category", "read_1":object, "read_2":object, 
                         "lane":"category", "batch_id":"category", 
                         "reads_bucket":"category", "results_bucket":"category", 
                         "results_prefix":object, "sequenced_datetime":object, 
                         "GenomeCov":float, "MeanDepth":float, "NumRawReads":int, 
                         "pcMapped":float, "Outcome":"category", "flag":"category",
                         "group":"category", "CSSTested":float, "matches":float,
                         "mismatches":float, "noCoverage":float, "anomalous":float})
    return df

def remove_duplicates(df, parameter="pcMapped"):
    """
        Drops duplicated submissions from df
    """
    return df.drop(get_indexes_to_remove(df, parameter)).reset_index(drop=True)

def get_indexes_to_remove(df, parameter):
    """
        Loops through unique submisions in the summary_df and collects indexes
        for duplicate submisions which should be excluded.

        Parameters:
            df (pandas dataframe object): a dataframe read from btb_wgs_samples.csv

            parameter (string): a column name from df on which to decide which 
            duplicate to keep, e.g. keep the submission with the largest pcMapped

        Returns:
            indexes (pandas index object): indexes to remove from dataframe
    """
    indexes = pd.Index([])
    for submission_no in df.submission.unique():
        parameter_max = df.loc[df["submission"]==submission_no][parameter].max()
        if len(df.loc[(df["submission"]==submission_no) & (df[parameter] == parameter_max)]) > 1:
            print(f"Submision {submission_no} is duplicated and has the same {parameter} value\n"
                    f"Skipping submision {submission_no}" )
            indexes = indexes.append(df.loc[df["submission"]==submission_no].index)
        else:
            indexes = indexes.append(df.loc[(df["submission"]==submission_no) & \
                (df[parameter] != parameter_max)].index)
    return indexes

def filter_samples(df, pcmap_threshold=(0,100), **kwargs):
    """ 
        Filters the sample summary dataframe which is based off 
        btb_wgs_samples.csv according to a set of criteria. 

        Parameters:
            df (pandas dataframe object): a dataframe read from btb_wgs_samples.csv.

            pcmap_threshold (tuple): min and max thresholds for pcMapped

            **kwargs: 0 or more optional arguments. Names must match a 
            column name in btb_wgs_samples.csv. If column is of type 
            'categorical' or 'object', vales must be of type 'list' 
            ispecifying a set of values to match against the argument 
            name's column in btb_wgs_samples.csv. For example, 
            'sample_name=["AFT-61-03769-21", "20-0620719"]' will include 
            just these two samples. If column is of type 'int' or 'float',
            values must be of type 'tuple' and of length 2, specifying a 
            min and max value for that column. 

        Returns:
            df (pandas dataframe object): a dataframe of 'Pass'
            only samples filtered according to criteria set out in 
            arguments.
    """
    df = filter_df_categorical(df, "Outcome", ["Pass"])
    df = filter_df_numeric(df, "pcMapped", pcmap_threshold)
    for column, values in kwargs.items():
        try:
            if pd.api.types.is_categorical_dtype(df[column]) or\
                    pd.api.types.is_object_dtype(df[column]):
                df = filter_df_categorical(df, column, values)
            elif pd.api.types.is_numeric_dtype(df[column]):
                df = filter_df_numeric(df, column, values)
        except KeyError as e:
            raise ValueError(f"Inavlid kwarg '{column}': must be one of: " 
                             f"{df.columns.to_list()}")
    if df.empty:
        raise Exception("0 samples meet specified criteria")
    return df
    
def filter_df_numeric(df, column_name, values):
    """ 
        Filters the summary dataframe according to the values in 
        'column_name' with min and max values defined by elements in
        'values'. The data in column name must me of dtype int or float
        and the argument 'values' must be of type list or tuple and 
        of length 2. 
    """ 
    if not pd.api.types.is_numeric_dtype(df[column_name]):
        raise InvalidDtype(dtype="float or int", column_name=column_name)
    if len(values) != 2:
        raise ValueError("pcmap_threshold must be of length 2")
    return df.loc[(df[column_name] > values[0]) & \
        (df[column_name] < values[1])].reset_index(drop=True)

def filter_df_categorical(df, column_name, values):
    """ 
        Filters the summary dataframe according to the values in 
        'column_name' retaining all rows which have summary_df[column_name]
        matching one on the elements in the input list 'values'. 
    """ 
    if not (pd.api.types.is_categorical_dtype(df[column_name]) or \
            pd.api.types.is_object_dtype(df[column_name])):
        raise InvalidDtype(dtype="category or object", column_name=column_name)
    if not isinstance(values, list):
        raise ValueError("Invalid kwarg value: must be of type list")
    return df.loc[df[column_name].isin(values)].reset_index(drop=True)

#TODO: unit test
def append_multi_fasta(s3_uri, outfile):
    """
        Appends a multi fasta file with the consensus sequence stored at s3_uri
        
        Parameters:
            s3_uris (list of tuples): list of s3 bucket and key pairs in tuple

            outfile (file object): file object refering to the multi fasta output
            file
    """
    with tempfile.TemporaryDirectory() as temp_dirname:
        consensus_filepath = os.path.join(temp_dirname, "temp.fas") 
        utils.s3_download_file(s3_uri[0], s3_uri[1], consensus_filepath)
        with open(consensus_filepath, 'rb') as consensus_file:
            outfile.write(consensus_file.read())

#TODO: unit test - use mocking
def build_multi_fasta(multi_fasta_path, bucket=DEFAULT_RESULTS_BUCKET,
                      summary_key=DEFAULT_SUMMARY_KEY, pcmap_threshold=(0,100), **kwargs):
    summary_df = summary_csv_to_df(bucket=bucket, summary_key=summary_key)
    summary_df = filter_samples(summary_df, pcmap_threshold=(80,100), **kwargs)
    summary_df = remove_duplicates(summary_df)   # - this potentially needs to be performed on the entire set to avoid duplicate samples across different trees i.e. higher up in the code
    summary_df.to_csv("/home/nickpestell/tmp/summary_test.csv")
    with open(multi_fasta_path, 'wb') as outfile:
        for _, sample in summary_df.iterrows():
            consensus_key = os.path.join(sample["results_prefix"], "consensus", 
                                         sample["sample_name"])
            append_multi_fasta((sample["results_bucket"], consensus_key+"_consensus.fas"), outfile)

#TODO: unit test maybe?
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
    multi_fasta_path = "/home/nickpestell/tmp/test_multi_fasta.fas"
    build_multi_fasta(multi_fasta_path)
    snps("/home/nickpestell/tmp/snps", multi_fasta_path)

if __name__ == "__main__":
    main()
