import subprocess

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

def s3_object_exists(bucket, key, endpoint_url):
    """
        Returns true if the S3 key is in the S3 bucket. False otherwise
        Thanks: https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    """
    
    key_exists = True
    s3 = boto3.resource('s3', endpoint_url=endpoint_url)
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