import subprocess
import os
import sys
import errno

import boto3
import botocore

def s3_folder_exists(bucket, path):
    """
        Returns true if the folder is in the S3 bucket. False otherwise
    """
    exists = False
    client = boto3.client('s3')
    path = os.path.join(path, "")
    response = client.list_objects(Bucket=bucket, Prefix=path, MaxKeys=1)
    if 'Contents' in response:
        exists = True
    return exists

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

def s3_download_folder(bucket, key, dest):
    """
        Downloads s3 folder at the key-bucket pair (strings) to dest 
        path (string)
    """
    if s3_folder_exists(bucket, key):
        key = os.path.join(key, "")
        bucket = os.path.join(bucket, "")
        dest = os.path.join(dest, "")
        run(["aws", "s3", "cp", "--recursive", "s3://" + bucket + key, dest])
    else:
        print("here")
        raise Exception("Path '{0}' does not exist in bucket '{1}'".format(key, bucket))