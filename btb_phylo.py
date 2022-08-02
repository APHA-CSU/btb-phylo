import argparse
import json
import os
from datetime import datetime

import btbphylo.utils as utils
import btbphylo.update_summary as update_summary
import btbphylo.consistify as consistify
import btbphylo.filter_samples as filter_samples
import btbphylo.phylogeny as phylogeny

def update_samples(results_path, summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH):
    """
        Updates the local copy of the sample summary csv file containing metadata 
        for all samples file or builds a new one from scratch if it does not 
        already exist. Downloads all FinalOut.csv files from s3-csu-003 and appends 
        them to the a pandas DataFrame and saves the data to csv.

        Parameters:
            summary_filepath (str): path to location of summary csv  
    """
    print("\nUpdate summary\n")
    print("\tLoading summary csv file ... \n")
    # download sample summary csv
    df_summary = update_summary.get_df_summary(summary_filepath)
    print("\tGetting s3 keys for batch summary files ... \n")
    # get s3 keys of FinalOut.csv for new batches of samples
    new_keys = update_summary.new_final_out_keys(df_summary)
    print("\tAppending new metadata to df_summary ... \n")
    # update the summary dataframe
    updated_df_summary, metadata = update_summary.append_df_summary(df_summary, new_keys)
    print("\tSaving summary csv file ... \n")
    # save summary to csv 
    utils.df_to_csv(updated_df_summary, summary_filepath)
    return (metadata,)

def sample_filter(results_path, summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH, 
                  config=False, **kwargs):
    """ 
        Filters the sample summary csv file 

        Parameters:
            summary_filepath (str): input path to location of summary csv  

            filtered_filepath (str): output path to location of filtered 
            summary csv  

            config (str): path to location of config json file

            **kwargs: 0 or more optional arguments. Names must match a 
            column name in btb_wgs_samples.csv. If column is of type 
            'categorical' or 'object', vales must be of type 'list' 
            ispecifying a set of values to match against the argument 
            name's column in btb_wgs_samples.csv. For example, 
            'sample_name=["AFT-61-03769-21", "20-0620719"]' will include 
            just these two samples. If column is of type 'int' or 'float',
            values must be of type 'tuple' and of length 2, specifying a 
            min and max value for that column. 

        Returns:
            df_filtered (pandas DataFrame object): a dataframe of 'Pass'
            only samples filtered according to criteria set out in 
            arguments.
    """
    filtered_filepath = os.path.join(results_path, "filtered_samples.csv")
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    if config:
        error_keys = [key for key, val in kwargs.items() if val]
        # if any arguments provided with --config
        if any(error_keys):
            raise ValueError(f"arguments '{', '.join(error_keys)}' are incompatible with "
                              "the 'config' argument")
        # parse config file
        with open(config) as f:
            filter_args = json.load(f)
    else:
        # remove unused filtering args
        filter_args = {k: v for k, v in kwargs.items() if v is not None}
    print("\nFilter samples\n")
    print("\tFiltering samples ... \n")
    # filter samples
    df_filtered, metadata = filter_samples.get_samples_df(summary_filepath, **filter_args)
    print("\tSaving filtered samples csv ... \n")
    # save filtered_df to csv
    utils.df_to_csv(df_filtered, filtered_filepath)
    return metadata, df_filtered

def consistify_samples(results_path, cattle_path, movement_path):
    print("\nConsistify\n")
    filtered_filepath = os.path.join(results_path, "filtered_samples.csv")
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    # make consistified folder
    consistified_outpath = os.path.join(results_path, "consistified")
    if not os.path.exists(consistified_outpath):
        os.makedirs(consistified_outpath)
    # make missing samples folder
    missing_samples_outpath = os.path.join(consistified_outpath, "missing_samples")
    if not os.path.exists(missing_samples_outpath):
        os.makedirs(missing_samples_outpath)
    # consistified file outpaths
    consistified_wgs_filepath = os.path.join(consistified_outpath, "consistified_wgs.csv")
    consistified_catte_filepath = os.path.join(consistified_outpath, "consistified_cattle.csv")
    consistified_movement_filepath = os.path.join(consistified_outpath, "consistified_movement.csv")
    missing_samples_dir = os.path.join(consistified_outpath, "missing_samples")
    # run consistify and save metadata in results root
    (metadata, wgs_consist) = consistify.consistify_csvs(filtered_filepath, cattle_path, movement_path,
                                                         consistified_wgs_filepath,  consistified_catte_filepath, 
                                                         consistified_movement_filepath, missing_samples_dir)
    return metadata, wgs_consist

def phylo(results_path, consensus_path, download_only=False, n_threads=1, 
          build_tree=False, df_filtered=None):
    """
        Runs phylogeny on filtered samples: Downloads consensus files, 
        concatonates into 1 large fasta file, runs snp-sites, runs snp-dists
        and runs megacc 

        Pramaters:
            results_path (str):  output path to results directory

            consenus_path (str):  output path to directory for saving consensus files

            download_only (bool): only download consensus (do not run phylogeny)

            n_threads (int): number of threads for snp-dists

            build_tree (bool): build a phylogentic tree with megacc

            filtered_filepath (str): optional input path to filtered samples csv

            filtered_df (pandas DataFrame object): optional dataframe containing 
            metadata for filtered samples
    """
    # if filtered_samples.csv in results folder: load csv
    filtered_filepath = os.path.join(results_path, "filter_samples.csv")
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    if os.path.exists(filtered_filepath):
        df_filtered = utils.summary_csv_to_df(filtered_filepath)
    # otherwise should be a pandas DataFrame
    elif df_filtered is not None:
        pass
    else:
        raise ValueError("If filtered_samples.csv does not exist in results_path ensure "
                         "that the filtered_df argument is provided")
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    # output paths
    multi_fasta_path = os.path.join(results_path, "multi_fasta.fas")
    snp_sites_outpath = os.path.join(results_path, "snps.fas")
    snp_dists_outpath = os.path.join(results_path, "snp_matrix.tab")
    tree_path = os.path.join(results_path, "mega")
    if not os.path.exists(tree_path):
        os.makedirs(tree_path)        
    print("\nPhylogeny\n")
    # concatonate fasta files
    phylogeny.build_multi_fasta(multi_fasta_path, df_filtered, consensus_path) 
    if not download_only:
        # run snp-sites
        print("\tRunning snp_sites ... \n")
        metadata = phylogeny.snp_sites(snp_sites_outpath, multi_fasta_path)
        # run snp-dists
        print("\tRunning snp_dists ... \n")
        phylogeny.build_snp_matrix(snp_dists_outpath, snp_sites_outpath, 
                                                      n_threads)
        if build_tree:
            # build tree
            print("\n\tRunning mega ... \n")
            phylogeny.build_tree(tree_path, snp_sites_outpath)
        return (metadata,)

def update_and_filter(results_path, summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH, 
                      **kwargs):
    """
        Updates local copy of sample summary csv, filters samples
        and saved the output filtered csv
    """
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    # update full sample summary
    metadata_update, *_ = update_samples(summary_filepath)
    # filter samples
    metadata_filter, df_filtered = sample_filter(results_path, summary_filepath, 
                                                 **kwargs)
    metadata = metadata_update
    metadata.update(metadata_filter)
    return metadata, df_filtered

def filter_and_phylo(results_path, consensus_path, summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH, 
                     n_threads=1, build_tree=False, download_only=False, **kwargs):
    """
        Filters samples and runs phylogeny 
    """
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    # filter samples
    metadata_filter, df_filtered = sample_filter(results_path, summary_filepath, **kwargs)
    # run phylogeny
    metadata_phylo, *_ = phylo(results_path, consensus_path, download_only, n_threads, build_tree, 
                               df_filtered=df_filtered)
    metadata = metadata_filter
    metadata.update(metadata_phylo)
    return (metadata,)

def full_pipeline(results_path, consensus_path, 
                  summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH, n_threads=1,
                  build_tree=False, download_only=False, viewbovine=False,
                  meta_path=None, **kwargs):
    """
        Updates local copy of summary csv file, filters samples and
        runs phylogeny 
    """
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    # update full sample summary and filter samples
    metadata_up_and_filt, df_filtered = update_and_filter(results_path, summary_filepath, 
                                                          **kwargs)
    # if running in ViewBovine must consistify datasets
    if viewbovine:
        if not meta_path:
            raise TypeError("Must provide keyword argument 'meta_path' if viewbovine=True")
        # cattle and movements csv filepaths
        cattle_filepath = f"{meta_path}/cattle.csv" 
        movements_filepath = f"{meta_path}/movements.csv" 
        # validate paths
        if not os.path.exists(cattle_filepath):
            raise FileNotFoundError(f"Can't find cattle.csv in {meta_path}")
        if not os.path.exists(movements_filepath):
            raise FileNotFoundError(f"Can't find movements.csv in {meta_path}")
        # consistify datasets for ViewBovine
        metadata_consist, df_consistified = consistify_samples(results_path, cattle_filepath, 
                                                               movements_filepath)
        # run phylogeny
        metadata_phylo, *_ = phylo(results_path, consensus_path, download_only, n_threads, 
                                   build_tree, df_filtered=df_consistified)
    else:
        # run phylogeny
        metadata_phylo = phylo(results_path, consensus_path, download_only, n_threads, build_tree, 
                               df_filtered=df_filtered)
    metadata = metadata_up_and_filt
    metadata.update(metadata_consist)
    metadata.update(metadata_phylo)
    return (metadata,) 

def main():
    parser = argparse.ArgumentParser(prog="btb-phylo")
    subparsers = parser.add_subparsers(help='sub-command help')

    # update complete summary csv
    subparser = subparsers.add_parser('update_samples', 
                                      help='updates a local copy of all sample metadata .csv file')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("--summary_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_SUMMARY_FILEPATH)
    subparser.set_defaults(func=update_samples)

    # filter samples
    subparser = subparsers.add_parser('filter', help='filters sample metadata .csv file')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("--summary_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_SUMMARY_FILEPATH)
    subparser.add_argument("--config", default=None, help="path to configuration file")
    subparser.add_argument("--sample_name", "-s", dest="Sample", nargs="+", help="optional filter")
    subparser.add_argument("--clade", "-c", dest="group", nargs="+", help="optional filter")
    subparser.add_argument("--outcome", dest="Outcome", nargs="+", help="optional filter")
    subparser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--genomecov", "-gc", dest="GenomeCov", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--n_count", "-nc", dest="Ncount", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--flag", "-f", dest="flag", nargs="+", help="optional filter")
    subparser.add_argument("--meandepth", "-md", dest="MeanDepth", type=float, nargs=2, help="optional filter")
    subparser.set_defaults(func=sample_filter)

    # consistify
    subparser = subparsers.add_parser('consistify', help='removes wgs samples that are missing from \
                                      cattle and movement data (metadata warehouse)')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("cattle_path", help="path to cattle csv")
    subparser.add_argument("movement_path", help="path to movements csv")
    subparser.set_defaults(func=consistify_samples)

    # run phylogeny
    subparser = subparsers.add_parser('phylo', help='performs phylogeny')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("consensus_path", help="path to where consensus files will be held")
    subparser.add_argument("--download_only", help="if only dowloading connsensus sequences",
                           action="store_true", default=False)
    subparser.add_argument("--n_threads", "-j", default=1, help="number of threads for snp-dists")
    subparser.add_argument("--build_tree", action="store_true", default=False, help="build a tree")
    subparser.set_defaults(func=phylo)

    # update and filter
    subparser = subparsers.add_parser('update_and_filter', 
                                      help='updates a metadata .csv file and filters samples')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("--summary_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_SUMMARY_FILEPATH)
    subparser.add_argument("--config", default=None, help="path to configuration file")
    subparser.add_argument("--sample_name", "-s", dest="Sample", nargs="+", help="optional filter")
    subparser.add_argument("--clade", "-c", dest="group", nargs="+", help="optional filter")
    subparser.add_argument("--outcome", dest="Outcome", nargs="+", help="optional filter")
    subparser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--genomecov", "-gc", dest="GenomeCov", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--n_count", "-nc", dest="Ncount", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--flag", "-f", dest="flag", nargs="+", help="optional filter")
    subparser.add_argument("--meandepth", "-md", dest="MeanDepth", type=float, nargs=2, help="optional filter")
    subparser.set_defaults(func=update_and_filter)

    # filter and phylo
    subparser = subparsers.add_parser('filter_and_phylo', help='filters samples and performs phylogeny')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("consensus_path", help="path to where consensus files will be held")
    subparser.add_argument("--summary_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_SUMMARY_FILEPATH)
    subparser.add_argument("--download_only", help="if only dowloading connsensus sequences",
                           action="store_true", default=False)
    subparser.add_argument("--n_threads", "-j", default=1, help="number of threads for snp-dists")
    subparser.add_argument("--build_tree", action="store_true", default=False, help="build a tree")
    subparser.add_argument("--config", default=None, help="path to configuration file")
    subparser.add_argument("--sample_name", "-s", dest="Sample", nargs="+", help="optional filter")
    subparser.add_argument("--clade", "-c", dest="group", nargs="+", help="optional filter")
    subparser.add_argument("--outcome", dest="Outcome", nargs="+", help="optional filter")
    subparser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--genomecov", "-gc", dest="GenomeCov", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--n_count", "-nc", dest="Ncount", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--flag", "-f", dest="flag", nargs="+", help="optional filter")
    subparser.add_argument("--meandepth", "-md", dest="MeanDepth", type=float, nargs=2, help="optional filter")
    subparser.set_defaults(func=filter_and_phylo)

    # full pipeline
    subparser = subparsers.add_parser('full_pipeline', help="runs the full phylogeny pipeline: updates full \
                                      samples summary, filters samples and performs phylogeny")
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("consensus_path", help = "path to where consensus files will be held")
    subparser.add_argument("--summary_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_SUMMARY_FILEPATH)
    subparser.add_argument("--download_only", help="if only dowloading connsensus sequences",
                           action="store_true", default=False)
    subparser.add_argument("--n_threads", "-j", default=1, help="number of threads for snp-dists")
    subparser.add_argument("--build_tree", action="store_true", default=False, help="build a tree")
    subparser.add_argument("--config", default=None, help="path to configuration file")
    subparser.add_argument("--sample_name", "-s", dest="Sample", nargs="+", help="optional filter")
    subparser.add_argument("--clade", "-c", dest="group", nargs="+", help="optional filter")
    subparser.add_argument("--outcome", dest="Outcome", nargs="+", help="optional filter")
    subparser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--genomecov", "-gc", dest="GenomeCov", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--n_count", "-nc", dest="Ncount", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--flag", "-f", dest="flag", nargs="+", help="optional filter")
    subparser.add_argument("--meandepth", "-md", dest="MeanDepth", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--viewbovine", "-vb", action="store_true", default=False, 
                           help="if running for ViewBovine production")
    subparser.add_argument("--meta_path", help="path to folder container cattle and movement data \
                           .csv files") 
    subparser.set_defaults(func=full_pipeline)

    # pasre args
    kwargs = vars(parser.parse_args())
    if not kwargs:
       parser.print_help()
       return
    # retrieve opperation
    func = kwargs.pop("func")
    # run
    metadata = {"datetime": str(datetime.now())}
    meta_update, *_ = func(**kwargs)
    # save metadata
    metadata.update(meta_update)
    print("\nsaving metadata ... \n")
    meta_filepath = os.path.join(kwargs["results_path"], "metadata.json")
    with open(meta_filepath, "w") as f:
        json.dump(metadata, f, indent=2)
    print("Done!\n")


if __name__ == "__main__":
    main() 
