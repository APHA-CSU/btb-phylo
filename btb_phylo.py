import tempfile
import os
import warnings
import re
import argparse
import json

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

def remove_duplicates(df, **kwargs):
    """
        Drops duplicated submissions from df based on the names and values of kwargs.
        Duplicated samples are kept if their entry in the column specified by the 
        kwarg name contains either the min or max value (specified by the kwarg value). 
        If more than one sample in the column name contain the min or max value then 
        the second kwarg is used to choose which of these duplicates to keep. This 
        pattern continues for n kwargs. If no more kwargs exist, the duplicated sample 
        appearing first in the dataframe is chosen.

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv

            **kwargs: 1 (minimum) or more optional arguments. The names of the 
            arguments must be a column name in df, the values must be of type str equal 
            to either "min" or "max".    

        Returns:
            df (pandas DataFrame object)

    """
    if not kwargs:
        raise TypeError("no kwargs provided, provide a column name and method for "
                        "dropping duplicates, e.g. pcMapped='min'")
    # reamining indexes: starts as all indexes
    remaining_indexes = df.index
    for column_name, method in kwargs.items():
        if column_name not in df.columns:
            raise ValueError(f"Invalid kwarg '{column_name}': must be one of: " 
                             f"{', '.join(df.columns.to_list())}")
        if not pd.api.types.is_numeric_dtype(df[column_name]):
            raise InvalidDtype(dtype="float or int", column_name=column_name)
        if method != "min" and method != "max":
            raise ValueError(f"Inavlid kwarg value: {column_name} must be either "
                             "'min' or 'max'")
        # get indexes to remove based on column_name and the selected method (min/max)
        indexes_to_remove = get_indexes_to_remove(df.loc[remaining_indexes], 
                                                  column_name, method)
        # update the remaining indexes by subtracting the indexes to remove
        remaining_indexes = remaining_indexes.difference(indexes_to_remove)
    # drop the indexes to remove
    return df.drop(df.index.difference(remaining_indexes)).drop_duplicates(["Submission"])

def get_indexes_to_remove(df, parameter, method):
    """
        Loops through unique submisions in the df and collects indexes for duplicate 
        submisions which should be excluded.

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv

            parameter (str): a column name from df on which to decide which 
            duplicate to keep, e.g. keep the submission with the largest pcMapped

            method (str): either 'min' or 'max', depending on whether the sample with
            the samllest or largest parameter value should be kept 

        Returns:
            indexes (pandas index object): indexes to remove from dataframe
    """
    indexes = pd.Index([])
    for submission_no in df.Submission.unique():
        if method == "max":
            threshold_1 = df.loc[df["Submission"]==submission_no][parameter].max()
        elif method == "min":
            threshold_1 = df.loc[df["Submission"]==submission_no][parameter].min()
        indexes = indexes.append(df.loc[(df["Submission"]==submission_no) & \
            (df[parameter] != threshold_1)].index)
    return indexes

def filter_df(df, **kwargs):
    """ 
        Filters the sample summary dataframe which is based off 
        btb_wgs_samples.csv according to a set of criteria. 

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv.

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
    numerical_kwargs = {}
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
    if numerical_kwargs:
        df_filtered = df.pipe(filter_columns_categorical, 
                            **categorical_kwargs).pipe(filter_columns_numeric, 
                                                        **numerical_kwargs)
    else:
        df_filtered = df.pipe(filter_columns_categorical, **categorical_kwargs)
    if len(df_filtered) < 2:
        raise Exception("1 or fewer samples meet specified criteria")
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

def get_samples_df(bucket=DEFAULT_RESULTS_BUCKET, summary_key=DEFAULT_SUMMARY_KEY, **kwargs):
    """
        Gets all the samples to be included in phylogeny. Loads btb_wgs_samples.csv
        into a pandas DataFrame. Filters the DataFrame arcording to criteria descriped in
        **kwargs. Removes Duplicated submissions.
    """
    # pipes the output DataFrame from summary_csv_to_df() (all samples) into filter_df()
    # into remove duplicates()
    # i.e. summary_csv_to_df() | filter_df() | remove_duplicates() > df
    df = utils.summary_csv_to_df(bucket=bucket, 
                                 summary_key=summary_key).pipe(filter_df,
                                                               **kwargs).pipe(remove_duplicates, 
                                                                              pcMapped="max", Ncount="min")
    
    return df

def append_multi_fasta(s3_bucket, s3_key, outfile, sample, consensus_path):
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
    consensus_filepath = consensus_path + sample['Sample'] + '.fas'
    if os.path.exists(consensus_filepath):
        pass
    else:
        # dowload consensus file from s3 to tempfile
        utils.s3_download_file(s3_bucket, s3_key, consensus_filepath)
    # writes to multifasta
    with open(consensus_filepath, 'rb') as consensus_file:
        outfile.write(consensus_file.read())

def build_multi_fasta(multi_fasta_path, df, consensus_path):
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
        count = 0
        num_samples = len(df)
        for index, sample in df.iterrows():
            count += 1
            print(f"adding sample: {count} / {num_samples}", end="\r")
            try:
                # extract the bucket and key of consensus file from s3 uri
                s3_bucket = extract_s3_bucket(sample["ResultLoc"])
                consensus_key = extract_s3_key(sample["ResultLoc"], sample["Sample"])
                # appends sample's consensus sequence to multifasta
                append_multi_fasta(s3_bucket, consensus_key, outfile, sample, consensus_path)
            except utils.NoS3ObjectError as e:
                # if consensus file can't be found in s3, btb_wgs_samples.csv must be corrupted
                print(e.message)
                print(f"Check results objects in row {index} of btb_wgs_sample.csv")
                raise e
        print(f"Added samples: {count} / {num_samples}")

def extract_s3_bucket(s3_uri):
    """
        Extracts s3 bucket name from an s3 uri using regex
    """
    # confirm s3 uri is correct and remove key from s3 uri
    sub_string = match_s3_uri(s3_uri)
    pattern = r's3-csu-\d{3,3}'
    # extract the bucket name
    matches = re.findall(pattern, sub_string)
    return matches[0]

def extract_s3_key(s3_uri, sample_name):
    """
        Generates an s3 key from an s3 uri and filename
    """
    # confirm s3 uri is correct 
    _ = match_s3_uri(s3_uri)
    pattern = r'^s3://s3-csu-\d{3,3}/+'
    # construct s3 key of consensus file
    return os.path.join(re.sub(pattern, "", s3_uri), 
                        "consensus", f"{sample_name}_consensus.fas")

def match_s3_uri(s3_uri):
    """
        Returns a substring s3 uri substring with the s3 key stripped away. 
        Raises BadS3UriError if the pattern is not found within the s3 uri.
    """
    pattern=r'^s3://s3-csu-\d{3,3}/+'
    matches = re.findall(pattern, s3_uri)
    if not matches:
        raise BadS3UriError(s3_uri)
    return matches[0]

def snp_sites(snp_sites_outpath, multi_fasta_path):
    """
        Run snp-sites on consensus files
    """
    # run snp sites 
    cmd = f'snp-sites {multi_fasta_path} -c -o {snp_sites_outpath}'
    utils.run(cmd, shell=True)

def build_snp_matrix(snp_dists_outpath, snp_sites_outpath, threads=1):
    """
        Run snp-dists
    """
    # run snp-dists
    cmd = f'snp-dists -j {threads} {snp_sites_outpath} > {snp_dists_outpath}'
    utils.run(cmd, shell=True)

def build_tree(tree_path, snp_sites_outpath):
    """
        Run mega
    """
    cmd = f'megacc -a infer_MP.mao -d {snp_sites_outpath} -o {tree_path}'
    utils.run(cmd, shell=True)

def main():
    # command line arguments
    parser = argparse.ArgumentParser(description="btb-phylo")
    parser.add_argument("results_path", help="path to results directory")
    parser.add_argument("--n_threads", "-j", type=str, default=1, help="number of threads for snp-dists")
    parser.add_argument("--build_tree", action="store_true", default=False)
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--sample_name", "-s", dest="Sample", type=str, nargs="+")
    parser.add_argument("--clade", "-c", dest="group", type=str, nargs="+")
    parser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2)
    parser.add_argument("--genomecov", "-gc", dest="GenomeCov", type=float, nargs=2)
    parser.add_argument("--n_count", "-nc", dest="Ncount", type=float, nargs=2)
    parser.add_argument("--flag", "-f", dest="flag", type=str, nargs="+")
    parser.add_argument("--meandepth", "-md", dest="MeanDepth", type=float, nargs=2)
    # parse agrs
    clargs = vars(parser.parse_args())
    # retreive "non-filtering" args
    results_path = clargs.pop("results_path")
    threads = clargs.pop("n_threads")
    tree = clargs.pop("build_tree")
    config = clargs.pop("config")
    # if config json file provided
    if config:
        error_keys = [key for key, val in clargs.items() if val]
        # if any arguments provided with --config
        if any(error_keys):
            raise parser.error(f"arguments '{', '.join(error_keys)}' are incompatible with "
                               "the 'config' argument")
        # parse config file
        with open(config) as f:
            kwargs = json.load(f)
    else:
        # remove unused filtering args
        kwargs = {k: v for k, v in clargs.items() if v is not None}
    # set output paths
    summary_csv_path = os.path.join(results_path, "sample_summary.csv")
    multi_fasta_path = os.path.join(results_path, "multi_fasta.fas")
    snp_sites_outpath = os.path.join(results_path, "snps.fas")
    snp_dists_outpath = os.path.join(results_path, "snp_matrix.tab")
    consensus_downloads = '/mnt/fsx-017/phyloConsensus/'
    tree_path = os.path.join(results_path, "mega")
    # get samples from btb_wgs_samples.csv and filter
    samples_df = get_samples_df("s3-staging-area", "nickpestell/btb_wgs_samples.csv", **kwargs)
    # save df_summary (samples to include in VB) to csv
    samples_df.to_csv(summary_csv_path)
    # concatonate fasta files
    build_multi_fasta(multi_fasta_path, samples_df, consensus_downloads) 
    # run snp-sites
    snp_sites(snp_sites_outpath, multi_fasta_path)
    # run snp-dists
    build_snp_matrix(snp_dists_outpath, snp_sites_outpath, threads)
    # build tree
    if tree:
        build_tree(tree_path, snp_sites_outpath)

if __name__ == "__main__":
    main()
