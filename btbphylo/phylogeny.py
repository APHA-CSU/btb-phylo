import re
import warnings
from os import path

import pandas as pd

import btbphylo.utils as utils

"""
    Performs phylogeny on specified samples: downloads samples, builds multi-fasta 
    and then runs snp-sites > snp-dists > megacc
"""

warnings.formatwarning = utils.format_warning


class BadS3UriError(Exception):
    def __init__(self, s3_uri):
        super().__init__()
        self.message = f"Incorrectly formatted s3 uri: '{s3_uri}'"

    def __str__(self):
        return self.message

def append_multi_fasta(s3_bucket, s3_key, outfile, sample, consensus_path):
    """
        Appends a multi fasta file with the consensus sequence stored at s3_uri
        
        Parameters:
            s3_bucket (string): s3 bucket of consensus file
            
            s3_key (string): s3 key of consensus file

            outfile (file object): file object refering to the multi fasta output
            file
    """
    # check if file is already present in the consensus directory 
    consensus_filepath = path.join(consensus_path , sample + '.fas')
    if not path.exists(consensus_filepath):
        # dowload consensus file from s3 to tempfile
        utils.s3_download_file_cli(s3_bucket, s3_key, consensus_filepath)
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
            print(f"\t\tadding sample: {count} / {num_samples}", end="\r")
            try:
                # extract the bucket and key of consensus file from s3 uri
                s3_bucket = extract_s3_bucket(sample["ResultLoc"])
                consensus_key = extract_s3_key(sample["ResultLoc"], sample["Sample"])
                # appends sample's consensus sequence to multifasta
                append_multi_fasta(s3_bucket, consensus_key, outfile, sample["Sample"], consensus_path)
            except utils.NoS3ObjectError as e:
                # if consensus file can't be found in s3, btb_wgs_samples.csv must be corrupted
                print(e.message)
                print(f"\tCheck results objects in row {index} of btb_wgs_sample.csv")
                raise e
        print(f"\t\tadded samples: {count} / {num_samples} \n")

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
    return path.join(re.sub(pattern, "", s3_uri), 
                        "consensus", f"{sample_name}_consensus.fas")

def match_s3_uri(s3_uri):
    """
        Returns an s3 uri substring with the s3 key stripped away. 
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
    # read the number of snps for metadata
    with open(snp_sites_outpath, "r") as f:
        next(f)
        for line in f:
            metadata = {"number_of_snps": len(line)-1}
            break
    return metadata

def build_snp_matrix(snp_dists_outpath, snp_sites_outpath, threads=1):
    """
        Run snp-dists
    """
    # run snp-dists
    cmd = f'snp-dists -c -j {threads} {snp_sites_outpath} > {snp_dists_outpath}'
    utils.run(cmd, shell=True)

def build_tree(tree_path, snp_sites_outpath):
    """
        Run mega
    """
    # count number of taxa 
    with open(snp_sites_outpath, "r") as f:
        for n_lines, _ in enumerate(f):
            if n_lines == 7:
                # if more than 3 samples run mega
                cmd = f'megacc -a infer_MP.mao -d {snp_sites_outpath} -o {tree_path}'
                utils.run(cmd, shell=True)
                break
    if n_lines < 7:
        warnings.warn("Unable to build tree! Need at least 4 taxa for tree building")

def post_process_snps_csv(snp_dists_outpath):
    """
        An I/O layer for post_process_snps_df. Changes the sample names in the 
        snp matrix at snp_dists_outpath to be consistent with cattle and movement 
        datasets. This is necessary for serving ViewBovine:
        Parses snp_matrix.csv.
        Runs post_process_snps_df().
        Saves post processed snp matrix to the same location as the input snp_matrix.csv
    """
    # load snp_matrix.csv
    snp_matrix = pd.read_csv(snp_dists_outpath, index_col=0)
    # process sample names
    processed_snp_matrix = post_process_snps_df(snp_matrix)
    # overwrite input snp_matrix.csv with processed snp_matrix
    processed_snp_matrix.to_csv(snp_dists_outpath)

def post_process_snps_df(snp_matrix):
    """
        Changes the sample names in the snp_matrix dataframe to be consistent with 
        cattle and movement datasets
    """
    # create copy of input dataframe
    processed_snp_matrix = snp_matrix.copy(deep=True)
    # extracts sample names and maps to new sample names
    new_sample_names = snp_matrix.index.map(process_sample_name)
    # updates the output dataframe with new sample names
    processed_snp_matrix.index = new_sample_names
    processed_snp_matrix.columns = new_sample_names
    return processed_snp_matrix

def process_sample_name(sample_name):
    """
        Changes the sample_name to be consistent with those in cattle and movement
        datasets. Strips the '_consensus' suffix and extracts submissions numbers
        from the sample names
    """
    # remove '_consensus' suffix
    submission_no = sample_name[:-10] if sample_name.endswith('_consensus') \
        else sample_name
    # extract submission number
    return utils.extract_submission_no(submission_no)
