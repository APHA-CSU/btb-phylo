import pandas as pd
import boto3

def list_keys(bucket_name, prefix):
    """ Returns a list of all keys matching a prefix in a S3 bucket """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    objects = bucket.objects.filter(Prefix=prefix)
    return [obj.key for obj in objects]

def bucket_summary(bucket, prefixes):
    """ summarises the samples in a bucket from a list of prefixes """
    # Get list of keys from s3
    keys = []
    for prefix in prefixes:
        keys.extend(list_keys(bucket, prefix))

    # Parse
    samples, batches, unpaired, not_parsed = pair_files(keys)

    # Include bucket name
    samples["bucket"] = bucket
    batches["bucket"] = bucket
    unpaired["bucket"] = bucket
    not_parsed["bucket"] = bucket

    return samples, batches, unpaired, not_parsed

def build():
    pass

def update():
    pass

if __name__ == "__main__":
    pass    
