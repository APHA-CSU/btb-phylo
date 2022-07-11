import tempfile
import re
import os
import argparse

import pandas as pd

import utils

DEFAULT_SUMMARY_BUCKET = "s3-csu-003"
DEFAULT_SUMMARY_KEY = "v3-2/btb_wgs_samples.csv"

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

def finalout_csv_to_df(s3_key, s3_bucket="s3-csu-003"):
    """
        Downloads FinalOut.csv files and writes to pandas dataframe
    """
    with tempfile.TemporaryDirectory() as temp_dirname:
        finalout_path = os.path.join(temp_dirname, "FinalOut.csv") 
        utils.s3_download_file(s3_bucket, s3_key, finalout_path)
        return pd.read_csv(finalout_path, comment="#")

def get_df_summary(bucket="s3-csu-003", key="v3-2/btb_wgs_samples.csv"):
    """
        Reads btb_wgs_samples.csv into pandas dataframe. Creates new empty dataframe
        if btb_wgs_samples.csv does not exist.
    """
    try:
        return utils.summary_csv_to_df(bucket, key)
    # if running for the first time (i.e. no btb_wgs_samples.csv), create new empty dataframe
    except utils.NoS3ObjectError:
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
    for key in s3_keys:
        prefix = "/".join(key.split("/")[:-1])
        result_loc = f"s3://s3-csu-003/{prefix}/"
        # if data not already summarised in df_summary, i.e. new data
        if result_loc not in list(df_summary["ResultLoc"]):
            # add the s3 key to new_keys
            new_keys.append(key)
    return new_keys

def extract_submission_no(sample_name):
    """ 
        Extracts submision number from sample name using regex 
    """
    pattern = r'\d{2,2}-\d{4,5}-\d{2,2}'
    matches = re.findall(pattern, sample_name)
    submission_no = matches[0] if matches else sample_name
    return submission_no

def add_submission_col(df):
    """
        Appends a 'Submission' number column to df
    """
    df["Submission"] = df["Sample"].map(extract_submission_no)
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
        print(f"downloading batch summary: {itteration+1} / {num_batches}", end="\r")
        # read FinalOut.csv for current key
        finalout_df = finalout_csv_to_df(new_keys[itteration]).pipe(add_submission_col)
        # append to df_summary
        df_summary = append_df_summary(pd.concat([df_summary, finalout_df]), 
                                       new_keys, itteration+1)
    else:
        print(f"downloaded batch summaries: {num_batches} / {num_batches} \n")
    return df_summary

def df_to_s3(df_summary, bucket="s3-csu-003", key="v3-2/btb_wgs_samples.csv"):
    """
        Upload df_summary to s3
    """
    with tempfile.TemporaryDirectory() as temp_dirname:
        summary_path = os.path.join(temp_dirname, "btb_wgs_samples.csv") 
        df_summary.to_csv(summary_path, index=False)
        utils.s3_upload_file(summary_path, bucket, key)

def main():
    # command line arguments
    parser = argparse.ArgumentParser(description="update summary")
    parser.add_argument("--summary_bucket", help="s3 bucket containing sample metadata .csv file", 
                        type=str, default=DEFAULT_SUMMARY_BUCKET)
    parser.add_argument("--summary_key", help="s3 key for sample metadata .csv file", 
                        type=str, default=DEFAULT_SUMMARY_KEY)
    args = parser.parse_args()
    print("\nupdate_summary\n")
    print("Downloading summary csv file ... \n")
    # download sample summary csv
    df_summary = get_df_summary(args.summary_bucket, args.summary_key)
    print("Getting list of s3 keys ... \n")
    # get s3 keys of FinalOut.csv for new batches of samples
    new_keys = new_final_out_keys(df_summary)
    print("Appending new metadata to df_summary ... ")
    # update the summary dataframe
    updated_df_summary = append_df_summary(df_summary, new_keys)
    print("Uploading summary csv file ... \n")
    # upload to s3
    df_to_s3(updated_df_summary, args.summary_bucket, args.summary_key)

if __name__ == "__main__":
    main()