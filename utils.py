import subprocess
import os
import sys
import errno

import boto3
import botocore

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
    return key_exists

def s3_download(bucket, key, dest):
    """
        Downloads s3 object at the key-bucket pair (strings) to dest 
        path (string)
    """
    #TODO: this is throwing an error which I currently don't understand
    if s3_object_exists(bucket, key):
        bucket = os.path.join(bucket, "")
        key = os.path.join(key, "")
        dest = os.path.join(dest, "")
        run(["aws", "s3", "cp", "--recursive", "s3://" + bucket + key, dest])
    else:
        print("s3 key '{0}' does not exist in bucket '{1}'".format(key, bucket))
        sys.exit(errno.EREMOTEIO)
