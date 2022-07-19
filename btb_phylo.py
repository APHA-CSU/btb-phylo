import argparse
import json
from os import path

import utils
import update_summary
import filter_samples
import phylogeny

def update_samples(summary_filepath):
    print("\nupdate_summary\n")
    print("Downloading summary csv file ... \n")
    # download sample summary csv
    df_summary = update_summary.get_df_summary(summary_filepath)
    print("Getting list of s3 keys ... \n")
    # get s3 keys of FinalOut.csv for new batches of samples
    new_keys = update_summary.new_final_out_keys(df_summary)
    print("Appending new metadata to df_summary ... ")
    # update the summary dataframe
    updated_df_summary = update_summary.append_df_summary(df_summary, new_keys)
    print("Saving summary csv file ... \n")
    # save summary to csv 
    update_summary.df_to_csv(updated_df_summary, summary_filepath)
    return updated_df_summary

def sample_filter()

def main():
    parser = argparse.ArgumentParser(prog="btb-phylo")
    subparsers = parser.add_subparsers(help='sub-command help')
    subparser = subparsers.add_parser('update_samples', 
                                      help='updates a local copy of all sample metadata .csv file')
    subparser.add_argument("--summary_filepath", help="path to sample metadata .csv file", 
                        default=utils.DEFAULT_SUMMARY_FILEPATH)
    subparser.set_defaults(func=update_samples)
    subparser = subparsers.add_parser('filter', 
                                      help='filters sample metadata .csv file')
    subparser.add_argument("filtered_filepath", help="path to output filtered sample metadata .csv file")
    subparser.add_argument("--summary_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_SUMMARY_FILEPATH)
    subparser.add_argument("--config", default=None)
    subparser.add_argument("--sample_name", "-s", dest="Sample", nargs="+")
    subparser.add_argument("--clade", "-c", dest="group", nargs="+")
    subparser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2)
    subparser.add_argument("--genomecov", "-gc", dest="GenomeCov", type=float, nargs=2)
    subparser.add_argument("--n_count", "-nc", dest="Ncount", type=float, nargs=2)
    subparser.add_argument("--flag", "-f", dest="flag", nargs="+")
    subparser.add_argument("--meandepth", "-md", dest="MeanDepth", type=float, nargs=2)
    subparser.set_defaults(func=sample_filter)
    # command line arguments
    parser = argparse.ArgumentParser(description="btb-phylo")
    parser.add_argument("results_path", help="path to results directory")
    parser.add_argument("consensus_path", help = "path to where consensus files will be held")
    parser.add_argument("--download_only", help = "if only dowloading connsensus sequences",
                        action="store_true", default=False)
    parser.add_argument("--n_threads", "-j", type=str, default=1, help="number of threads for snp-dists")
    parser.add_argument("--build_tree", action="store_true", default=False)
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--sample_name", "-s", dest="Sample", type=str, nargs="+")
    parser.add_argument("--clade", "-c", dest="group", type=str, nargs="+")
    parser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2)
    parser.add_argument("--genomecov", "-gc", dest="GenomeCov", type=float, nargs=2)
    parser.add_argument("--n_count", "-nc", dest="Ncount", type=float, nargs=2)
    parser.add_argument("--flag", "-f", dest="flag", type=str, nargs="+")
    parser.add_argument("--meandepth", "-md", dest="MeanDepth", type=float, nargs=2)
    # parse agrs
    clargs = vars(parser.parse_args())
    # retreive "non-filtering" args
    results_path = clargs.pop("results_path")
    consensus_path = clargs.pop("consensus_path")
    download_only = clargs.pop("download_only")
    threads = clargs.pop("n_threads")
    tree = clargs.pop("build_tree")
    config = clargs.pop("config")
    # if config json file provided
    if config:
        error_keys = [key for key, val in clargs.items() if val]
        # if any arguments provided with --config
        if any(error_keys):
            raise parser.error(f"arguments '{', '.join(error_keys)}' are incompatible with "
                               "the 'config' argument")
        # parse config file
        with open(config) as f:
            kwargs = json.load(f)
    else:
        # remove unused filtering args
        kwargs = {k: v for k, v in clargs.items() if v is not None}
    # set output paths
    filtered_filepath = path.join(results_path, "sample_summary.csv")
    multi_fasta_path = path.join(results_path, "multi_fasta.fas")
    snp_sites_outpath = path.join(results_path, "snps.fas")
    snp_dists_outpath = path.join(results_path, "snp_matrix.tab")
    tree_path = path.join(results_path, "mega")
    # get samples from btb_wgs_samples.csv and filter
    samples_df = filter_samples.get_samples_df("s3-staging-area", "nickpestell/btb_wgs_samples.csv", **kwargs)
    # save df_summary (samples to include in VB) to csv
    samples_df.to_csv(filtered_filepath, index=False)
    # concatonate fasta files
    phylogeny.build_multi_fasta(multi_fasta_path, samples_df, consensus_path) 
    if not download_only:
        # run snp-sites
        phylogeny.snp_sites(snp_sites_outpath, multi_fasta_path)
        # run snp-dists
        phylogeny.build_snp_matrix(snp_dists_outpath, snp_sites_outpath, threads)
        # build tree
        if tree:
            phylogeny.build_tree(tree_path, snp_sites_outpath)
    pass

if __name__ == "__main__":
    main() 