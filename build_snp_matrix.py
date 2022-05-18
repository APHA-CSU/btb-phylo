import tempfile
import os

import utils

def build_multi_fasta(plates_csv, bucket="s3-csu-003", keys=None):
    """
    # keys is a list of s3_uris - for bespoke requests
    # if None download on plate-by-plate basis
    # TODO: ensure there are no duplicates - need policy for choosing the correct sample, probably
    # TODO: make filenames consistent - many have "consensus.fas" at the end
    # based on highes pc-mapped.
    if keys == None:
        for plate_id in plate_ids:
            s3_uris = [uri for uri in plates_csv]
            append_multi_fasta(multi_fasta_path, s3_uris)
    else:
        append_multi_fasta(multi_fasta_path, keys)
    """
    with tempfile.TemporaryDirectory() as consensus_dirname:
        append_multi_fasta(s3_uris, consensus_dirname)

def append_multi_fasta(s3_uris, multi_fasta_path):
    '''
    Builds a file appended with consensus sequences of samples for phylogeny
    
    Parameters:
        s3_uris (list of tuples): list of s3 bucket and key pairs in tuple
        multi_fasta_path (string): local filepath for output file
    '''
    with open(multi_fasta_path, 'wb') as out_file:
        for s3_uri in s3_uris:
            with tempfile.TemporaryDirectory() as temp_dirname:
                consensus_filepath = os.path.join(temp_dirname, "temp.fas") 
                utils.s3_download_file(s3_uri[0], s3_uri[1], consensus_filepath)
                with open(consensus_filepath, 'rb') as consensus_file:
                    out_file.write(consensus_file.read())

def snps(output_path, multi_fasta_path):
    """run snp-sites on consensus files, then runs snp-dists on the results"""

    # run snp sites 
    cmd = f'snp-sites {multi_fasta_path} -c -o {output_path}snpsites.fas'
    utils.run(cmd, shell=True)

    # run snp-dists
    cmd = f'snp-dists {output_path}snpsites.fas > {output_path}snps.tab'
    utils.run(cmd, shell=True)

def main():
    # parse plates.csv
    # parse outcomes.csv -> for determining if samples should be included: output from btb-seq (cam)
    # build multi-fasta
    # run snp-sites
    # run snp-dist
    s3_uris = [("s3-staging-area", "nickpestell/#"), ("s3-staging-area", "nickpestell/AF-21-05371-19_consensus.fas")]
    with tempfile.TemporaryDirectory() as temp_dirname:
        append_multi_fasta(s3_uris, os.path.join(temp_dirname, "multi-fasta.fas"))

if __name__ == "__main__":
    main()
