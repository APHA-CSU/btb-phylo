import utils

def build_multi_fasta(plates_csv, bucket="s3-csu-003", keys=None):
    """
    # keys is a list of s3_uris - for bespoke requests
    # if None download on plate-by-plate basis
    # TODO: ensure there are no duplicates - need policy for choosing the correct sample, probably
    # based on highes pc-mapped.
    if keys == None:
        for plate_id in plate_ids:
            s3_uris = [uri for uri in plates_csv]
            append_multi_fasta(multi_fasta_path, s3_uris)
    else:
        append_multi_fasta(multi_fasta_path, keys)
    """
    pass

def append_multi_fasta(multi_fasta_path, s3_uris):
    """
    1. dowloads consensus files from s3-csu-003 to a tmp directory
    2. writes (appends) to multi fasta file - assuming open() doesn't load the whole file into memory - this will be around ~50Gb 
        for the entire dataset
    3. deletes tmp directory
    """
    pass

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
    pass

if __name__ == "__main__":
    main()
