import tempfile
from os import path

import pandas as pd

import btbphylo.utils as utils

"""
    Updates the a local csv file containing metadata for all wgs samples 
    in s3-csu-001
"""

def get_finalout_s3_keys(bucket="s3-csu-003", prefix="v3-2"):
    """
        Returns a list of s3 keys for all FinalOut.csv files stored under the 
        given prefix.
    """
    cmd = f'aws s3 ls s3://{bucket}/{prefix}/ --recursive | grep -e ".*FinalOut.*"'
    # direct output of cmd subprocess into finalout_s3_data
    finalout_s3_data = utils.run(cmd, shell=True, capture_output=True)
    # extract s3 key from output of cmd
    return list(map(extract_s3_key, finalout_s3_data.split("\n")))

def extract_s3_key(final_out_s3_object):
    """
        Extracts the s3 key from s3 metadata of a single FinalOut.csv file
    """
    return final_out_s3_object.split(" ")[-1]

def finalout_s3_to_df(s3_key, s3_bucket="s3-csu-003"):
    """
        Downloads FinalOut.csv files and writes to pandas dataframe
    """
    with tempfile.TemporaryDirectory() as temp_dirname:
        finalout_path = path.join(temp_dirname, "FinalOut.csv") 
        utils.s3_download_file_cli(s3_bucket, s3_key, finalout_path)
        return utils.finalout_csv_to_df(finalout_path)

def get_df_summary(summary_filepath=utils.DEFAULT_WGS_SAMPLES_FILEPATH):
    """
        Reads summary csv into pandas dataframe. Creates new empty dataframe
        if local copy of summary csv does not exist.
    """
    if path.exists(summary_filepath):
        return utils.summary_csv_to_df(summary_filepath)
    # if running for the first time (i.e. no btb_wgs_samples.csv), create new empty dataframe
    else:
        column_names = ["Sample", "GenomeCov", "MeanDepth", "NumRawReads", "pcMapped", 
                        "Outcome", "flag", "group", "CSSTested", "matches","mismatches", 
                        "noCoverage", "anomalous", "Ncount", "ResultLoc", "ID", 
                        "TotalReads", "Abundance", "Submission"]
        return pd.DataFrame(columns=column_names)

def new_final_out_keys(df_summary):
    """
        Returns a list of s3_keys for FinalOut.csv files not currently in the
        summary csv file, i.e. new data.
    """
    # get list of all FinalOut.csv s3 keys
    s3_keys = get_finalout_s3_keys()
    new_keys = []
    old_result_loc = set(df_summary["ResultLoc"])
    for key in s3_keys:
        prefix = "/".join(key.split("/")[:-1])
        result_loc = f"s3://s3-csu-003/{prefix}/"
        # if data not already summarised in df_summary, i.e. new data
        if result_loc not in old_result_loc:
            # add the s3 key to new_keys
            new_keys.append(key)
    return new_keys

def add_submission_col(df):
    """
        Appends a 'Submission' number column to df
    """
    df["Submission"] = df["Sample"].map(utils.extract_submission_no)
    return df

def append_df_summary(df_summary, new_keys, itteration=0):
    """
        Appends new FinalOut.csv data (with additional submission number)
        to the df_summary.

        Parameters:
            df_summary (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv

            new_keys (list): a list of s3 keys for FinalOut.csv files of all new data, 
            i.e. data not currently summarised in wgs_samples.csv

            itteration (int): the current itteration for reccursive count

        Returns: 
            df_summary (pandas DataFrame object): an updated dataframe with new wgs sample
            metadata added
    """
    # if not yet on last itteration (last new_key element)
    num_batches = len(new_keys)
    if itteration < num_batches:
        print(f"\t\tdownloading batch summary: {itteration+1} / {num_batches}", end="\r")
        # read FinalOut.csv for current key
        finalout_df = finalout_s3_to_df(new_keys[itteration]).pipe(add_submission_col)
        # append to df_summary
        df_summary, _ = append_df_summary(pd.concat([df_summary, finalout_df]), 
                                          new_keys, itteration+1)
    else:
        print(f"\t\tdownloaded batch summaries: {num_batches} / {num_batches} \n")
    metadata = {"total_number_of_wgs_samples": len(df_summary)}
    return df_summary, metadata
