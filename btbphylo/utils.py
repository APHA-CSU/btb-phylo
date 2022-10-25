import subprocess
from os import path
import re

import boto3
import botocore
import pandas as pd


"""
    Utility functions
"""

DEFAULT_WGS_SAMPLES_FILEPATH = path.join(path.dirname(path.dirname(path.abspath(__file__))), 
                                         "all_wgs_samples.csv")


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

class NoS3ObjectError(Exception):
    def __init__(self, bucket, key):
        super().__init__()
        self.message = f"'{key}' does not exist in bucket '{bucket}'"

    def __str__(self):
        return self.message

def format_warning(message, category, filename, lineno, file=None, line=None):
    return '%s:%s: %s:%s\n' % (filename, lineno, category.__name__, message)

def wgs_csv_to_df(summary_filepath):
    """
        Read sample summary CSV and returns the data in a pandas dataframe.
    """
    df = pd.read_csv(summary_filepath, comment="#", 
                     dtype = {"Sample":"category", "GenomeCov":float, 
                     "MeanDepth":float, "NumRawReads":float, "pcMapped":float, 
                     "Outcome":"category", "flag":"category", "group":"category", 
                     "CSSTested":float, "matches":float, "mismatches":float, 
                     "noCoverage":float, "anomalous":float, "Ncount":float, 
                     "ResultLoc":"category", "ID":"category", "TotalReads":float, 
                     "Abundance":float, "Submission": object})
    return df

def finalout_csv_to_df(finalout_filepath):
    """
        Reads finalout CSV and returns the data in a pandas dataframe.
    """
    df = pd.read_csv(finalout_filepath, comment="#", 
                     dtype = {"Sample":"category", "GenomeCov":float, 
                     "MeanDepth":float, "NumRawReads":float, "pcMapped":float, 
                     "Outcome":"category", "flag":"category", "group":"category", 
                     "CSSTested":float, "matches":float, "mismatches":float, 
                     "noCoverage":float, "anomalous":float, "Ncount":float, 
                     "ResultLoc":"category", "ID":"category", "TotalReads":float, 
                     "Abundance":float})
    return df

def extract_submission_no(sample_name):
    """ 
        Extracts submision number from sample name using regex. 
        Prepends 'AF-' to submission number.
        Converts all lower case to upper case letters. 
    """
    pattern = r'\d{2,2}-\d{4,5}-\d{2,2}'
    matches = re.findall(pattern, sample_name)
    submission_no = f"AF-{matches[0]}" if matches else sample_name
    return submission_no.upper()

# TODO: remove if unused
def s3_folder_exists(bucket, path):
    """
        Returns true if the folder is in the S3 bucket. False otherwise
    """
    exists = False
    client = boto3.client('s3')
    path = path.join(path, "")
    response = client.list_objects(Bucket=bucket, Prefix=path, MaxKeys=1)
    if 'Contents' in response:
        exists = True
    return exists

def s3_object_exists(bucket, key):
    """
        Returns true if the S3 key is in the S3 bucket. False otherwise
        Thanks: https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    """
    key_exists = True
    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            key_exists = False
        else:
            # Something else has gone wrong.
            raise e
    return  key_exists

def run(cmd, *args, **kwargs):
    """ Run a command and assert that the process exits with a non-zero exit code.
        See python's subprocess.run command for args/kwargs

        Parameters:
            cmd (list): List of strings defining the command, see (subprocess.run in python docs)
            cwd (str): Set surr

        Returns:
            None: if capture_output == False (default)
            process' stdout (str): if capture_output == True 
    """
    # TODO: store stdout to a file
    ps = subprocess.run(cmd, *args, **kwargs)
    returncode = ps.returncode
    if returncode:
        raise Exception("""*****
            %s
            cmd failed with exit code %i
          *****""" % (cmd, returncode))
    if "capture_output" in kwargs and kwargs["capture_output"]:
        return ps.stdout.decode().strip('\n')

# TODO: remove if unused
def s3_download_folder(bucket, key, dest):
    """
        Downloads s3 folder at the key-bucket pair (strings) to dest 
        path (string)
    """
    if s3_folder_exists(bucket, key):
        key = path.join(key, "")
        bucket = path.join(bucket, "")
        dest = path.join(dest, "")
        run(["aws", "s3", "cp", "--recursive", "s3://" + bucket + key, dest])
    else:
        raise NoS3ObjectError(bucket, key)

# TODO: remove if unused
def s3_download_file(bucket, key, dest):
    """
        Downloads s3 folder at the key-bucket pair (strings) to dest 
        path (string) using boto3
    """
    if s3_object_exists(bucket, key):
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, dest)
    else:
        raise NoS3ObjectError(bucket, key)

def s3_download_file_cli(bucket, key, dest):
    """
        Downloads s3 folder at the key-bucket pair (strings) to dest 
        path (string) using the AWS CLI
    """
    if s3_object_exists(bucket, key):
        run(["aws", "s3", "cp", f"s3://{bucket}/{key}", dest], capture_output=True)
    else:
        raise NoS3ObjectError(bucket, key)

def s3_upload_file(file, bucket, key):
    s3_client = boto3.client('s3')
    s3_client.upload_file(file, bucket, key)

# TODO: remove if unused
def list_s3_objects(bucket, prefix):
    """
        Return a list of s3 objects with the common prefix (argument)
    """
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Delimiter = '/', Prefix=prefix)
    return [i['Prefix'] for i in response['CommonPrefixes']]

def df_to_csv(df_wgs, summary_filepath=DEFAULT_WGS_SAMPLES_FILEPATH):
    """
        Save df_wgs to csv
    """
    df_wgs.to_csv(summary_filepath, index=False)
