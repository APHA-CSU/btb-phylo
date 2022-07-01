import tempfile
import os
import pandas as pd
import warnings
import re

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
            self.message = f"Invalid series name '{kwargs['column_name']}'. Series must be of type {kwargs['dtype']}"
        else:
            self.message = message

    def __str__(self):
        return self.message

class BadS3UriError(Exception):
    def __init__(self, s3_uri):
        super().__init__()
        self.message = f"Incorrectly formatted s3 uri: '{s3_uri}'"

    def __str__(self):
        return self.message

def summary_csv_to_df(bucket, summary_key):
    """
        Downloads btb_wgs_samples.csv and returns the data in a pandas dataframe.
    """
    with tempfile.TemporaryDirectory() as temp_dirname:
        summary_filepath = os.path.join(temp_dirname, "samples.csv")
        utils.s3_download_file(bucket, summary_key, summary_filepath)
        df = pd.read_csv(summary_filepath, comment="#", 
                         dtype = {"Sample":"category", 
                         "GenomeCov":float, "MeanDepth":float, "NumRawReads":float, 
                         "pcMapped":float, "Outcome":"category", "flag":"category",
                         "group":"category", "CSSTested":float, "matches":float,
                         "mismatches":float, "noCoverage":float, "anomalous":float,
                         "Ncount":float, "submission":"object", 
                         "ResultLoc":"category", "ID":"category",
                         "TotalReads":float, "Abundance":float})
    return df

def remove_duplicates(df, parameter="pcMapped"):
    """
        Drops duplicated submissions from df
    """
    return df.drop(get_indexes_to_remove(df, parameter))

def get_indexes_to_remove(df, parameter):
    """
        Loops through unique submisions in the summary_df and collects indexes
        for duplicate submisions which should be excluded.

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv

            parameter (str): a column name from df on which to decide which 
            duplicate to keep, e.g. keep the submission with the largest pcMapped

        Returns:
            indexes (pandas index object): indexes to remove from dataframe
    """
    indexes = pd.Index([])
    for submission_no in df.submission.unique():
        parameter_max = df.loc[df["submission"]==submission_no][parameter].max()
        if len(df.loc[(df["submission"]==submission_no) & (df[parameter] == parameter_max)]) > 1:
            warnings.warn(f"Submision {submission_no} is duplicated and has the same "
                          f"{parameter} value\nSkipping submision {submission_no}")
            # if duplicate submissions share the maximum paramter value add all 
            # entries with submission_no to the list of indexes to remove
            indexes = indexes.append(df.loc[df["submission"]==submission_no].index)
        else:
            # otherwise add all duplicates except for the one with the maximum 
            # parameter value
            indexes = indexes.append(df.loc[(df["submission"]==submission_no) & \
                (df[parameter] != parameter_max)].index)
    return indexes

def filter_df(df, pcmap_threshold=(0,100), **kwargs):
    """ 
        Filters the sample summary dataframe which is based off 
        btb_wgs_samples.csv according to a set of criteria. 

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv.

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
            df_filtered (pandas DataFrame object): a dataframe of 'Pass'
            only samples filtered according to criteria set out in 
            arguments.

            ValueError: if any kwarg is not in df.columns  
    """
    # add "Pass" only samples and pcmap_theshold to the filtering 
    # criteria by default
    categorical_kwargs = {"Outcome": ["Pass"]}
    numerical_kwargs = {"pcMapped": pcmap_threshold}
    for key in kwargs.keys():
        if key not in df.columns:
            raise ValueError(f"Invalid kwarg '{key}': must be one of: " 
                             f"{', '.join(df.columns.to_list())}")
        else:
            if pd.api.types.is_categorical_dtype(df[key]) or \
                    pd.api.types.is_object_dtype(df[key]):
                # add categorical columns in **kwargs to categorical_kwargs
                categorical_kwargs[key] = kwargs[key]
            else:
                # add numerical columns in **kwargs to numerical_kwargs
                numerical_kwargs[key] = kwargs[key]
    # calls filter_columns_catergorical() with **categorical_kwargs on df, pipes 
    # the output into filter_columns_numeric() with **numerical_kwargs and assigns
    # the output to a new df_filtered
    df_filtered = df.pipe(filter_columns_categorical, 
                          **categorical_kwargs).pipe(filter_columns_numeric, 
                                                     **numerical_kwargs)
    if df_filtered.empty:
        raise Exception("0 samples meet specified criteria")
    return df_filtered
    
# TODO: raise exception if second element is smaller than first
def filter_columns_numeric(df, **kwargs):
    """ 
        Filters the summary dataframe according to kwargs, where keys
        are the columns on which to filter and the values must be tuple 
        of length 2 with min and max thresholds in elements 0 and 1. 
        The data in column name must me of dtype int or float. 
    """ 
    for column_name, value in kwargs.items():
        # ensures that column_names are of numeric type
        if not pd.api.types.is_numeric_dtype(df[column_name]):
            raise InvalidDtype(dtype="float or int", column_name=column_name)
        # ensures that values are of length 2 (min & max) and numeric
        if (not isinstance(value, list) and not isinstance(value,tuple)) or len(value) != 2 \
            or (not isinstance(value[0], float) and not isinstance(value[0], int)) \
            or (not isinstance(value[1], float) and not isinstance(value[1], int)) \
            or value[0] >= value[1]:
            raise ValueError(f"Invalid kwarg '{column_name}': must be list or tuple of 2" 
                              " numbers where the 2nd element is larger than the 1st")
    # constructs a query string on which to query df; e.g. 'pcMapped >= 90 and 
    # pcMapped <= 100 and GenomeCov >= 80 and GenomveCov <= 100'
    query = ' and '.join(f'{col} >= {vals[0]} and {col} <= {vals[1]}' \
        for col, vals in kwargs.items())
    return df.query(query)

def filter_columns_categorical(df, **kwargs):
    """ 
        Filters the summary dataframe according to kwargs, where keys
        are the columns on which to filter and the values are lists containing
        the values of df[kwarg[key]] to retain. 
    """ 
    for column_name, value in kwargs.items():
        # ensures that column_names are of type object or categorical
        if not (pd.api.types.is_categorical_dtype(df[column_name]) or \
            pd.api.types.is_object_dtype(df[column_name])):
            raise InvalidDtype(dtype="category or object", column_name=column_name)
        # ensures that values are list of strings
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError(f"Invalid kwarg '{column_name}': must be a list of strings")
        # issues a warning if any value is missing from specified column
        missing_values = [item for item in value if item not in list(df[column_name])]
        if missing_values:
            warnings.warn(f"Column '{column_name}' does not contain the values "
                          f"'{', '.join(missing_values)}'")
    # constructs a query string on which to query df; e.g. 'Outcome in [Pass] and 
    # sample_name in ["AFT-61-03769-21", "20-0620719"]. 
    query = ' and '.join(f'{col} in {vals}' for col, vals in kwargs.items())
    return df.query(query)

def get_samples_df(bucket=DEFAULT_RESULTS_BUCKET, summary_key=DEFAULT_SUMMARY_KEY, 
                   pcmap_threshold=(0,100), **kwargs):
    """
        Gets all the samples to be included in phylogeny. Loads btb_wgs_samples.csv
        into a pandas DataFrame. Filters the DataFrame arcording to criteria descriped in
        **kwargs. Removes Duplicated submissions.
    """
    # pipes the output DataFrame from summary_csv_to_df() (all samples) into filter_df()
    # into remove duplicates()
    # i.e. summary_csv_to_df() | filter_df() | remove_duplicates() > df
    df = summary_csv_to_df(bucket=bucket, 
                           summary_key=summary_key).pipe(filter_df, pcmap_threshold=pcmap_threshold, 
                                                         **kwargs).pipe(remove_duplicates)
    return df

def append_multi_fasta(s3_bucket, s3_key, outfile):
    """
        Appends a multi fasta file with the consensus sequence stored at s3_uri
        
        Parameters:
            s3_bucket (string): s3 bucket of consensus file
            
            s3_key (string): s3 key of consensus file

            outfile (file object): file object refering to the multi fasta output
            file
    """
    # temp directory for storing individual consensus files - deleted when function
    # returns
    with tempfile.TemporaryDirectory() as temp_dirname:
        consensus_filepath = os.path.join(temp_dirname, "temp.fas") 
        # dowload consensus file from s3 to tempfile
        utils.s3_download_file(s3_bucket, s3_key, consensus_filepath)
        # writes to multifasta
        with open(consensus_filepath, 'rb') as consensus_file:
            outfile.write(consensus_file.read())

def build_multi_fasta(multi_fasta_path, df):
    """
        Builds the multi fasta constructed from consensus sequences for all 
        samples in df

        Parameters:
            multi_fasta_path (str): path for location of multi fasta sequence
            (appended consensus sequences for all samples)

            df (pandas DataFrame object): dataframe containing s3_uri for 
            consensus sequences of samples to be included in phylogeny

        Raises:
            utils.NoS3ObjectError: if the object cannot be found in the 
            specified s3 bucket
    
    """
    with open(multi_fasta_path, 'wb') as outfile:
        # loops through all samples to be included in phylogeny
        for index, sample in df.iterrows():
            try:
                # extract the bucket and key of consensus file from s3 uri
                s3_bucket = extract_s3_bucket(sample["ResultLoc"])
                consensus_key = extract_s3_key(sample["ResultLoc"], sample["Sample"])
                # appends sample's consensus sequence to multifasta
                append_multi_fasta(s3_bucket, consensus_key, outfile)
            except utils.NoS3ObjectError as e:
                # if consensus file can't be found in s3, btb_wgs_samples.csv must be corrupted
                print(e.message)
                print(f"Check results objects in row {index} of btb_wgs_sample.csv")
                raise e

# TODO: unittest
def extract_s3_bucket(s3_uri):
    """
        Extracts s3 bucket name from an s3 uri using regex
    """
    pattern = r'^s3://s3-csu-\d{3,3}/'
    matches = re.findall(pattern, s3_uri)
    if matches:
        sub_pattern = r's3-csu-\d{3,3}'
        sub_matches = re.findall(sub_pattern, matches[0])
    else:
        raise BadS3UriError(s3_uri)
    return sub_matches[0]

# TODO: unittest
def extract_s3_key(s3_uri, sample_name):
    """
        Generates an s3 key from an s3 uri and filename
    """
    pattern = r'^s3://s3-csu-\d{3,3}/'
    matches = re.findall(pattern, s3_uri)
    if not matches:
        raise BadS3UriError(s3_uri)
    return os.path.join(re.sub(pattern, "", s3_uri), 
                        "consensus", f"{sample_name}_consensus.fas")

def snp_sites(snp_sites_outpath, multi_fasta_path):
    """
        Run snp-sites on consensus files
    """
    # run snp sites 
    cmd = f'snp-sites {multi_fasta_path} -c -o {snp_sites_outpath}'
    utils.run(cmd, shell=True)

def build_snp_matrix(snp_dists_outpath, snp_sites_outpath):
    """
        Run snp-dists
    """
    # run snp-dists
    cmd = f'snp-dists {snp_sites_outpath} > {snp_dists_outpath}'
    utils.run(cmd, shell=True)

def build_tree(tree_path, snp_sites_outpath):
    """
        Run mega
    """
    cmd = f'megacc -a infer_MP.mao -d {snp_sites_outpath} -o {tree_path}'
    utils.run(cmd, shell=True)

def main():
    multi_fasta_path = "/home/nickpestell/tmp/test_multi_fasta.fas"
    results_path = "/home/nickpestell/tmp/"
    samples_df = get_samples_df("s3-staging-area", "nickpestell/summary_v4.csv")
    # TODO: make multi_fasta_path a tempfile and pass file object into build_multi_fasta
    snp_sites_outpath = os.path.join(results_path, "snps.fas")
    snp_dists_outpath = os.path.join(results_path, "snp_matrix.tab")
    tree_path = os.path.join(results_path, "mega")
    build_multi_fasta(multi_fasta_path, samples_df)
    snp_sites(snp_sites_outpath, multi_fasta_path)
    build_snp_matrix(snp_dists_outpath, snp_sites_outpath)
    #build_tree(tree_path, snp_sites_outpath)

if __name__ == "__main__":
    main()