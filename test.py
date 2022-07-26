import boto3
import botocore
import btbphylo.utils as utils

boto3.set_stream_logger('')
utils.s3_download_file("s3-csu-003", "v3-2/Results_10049_27Jun22/consensus/AF-12-01212-17_consensus.fas", "/home/nickpestell/tmp/test.fas")
