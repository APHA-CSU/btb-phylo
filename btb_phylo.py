import argparse
import json
import os
import subprocess
import shutil
import sys
import tempfile
from datetime import datetime

import pandas as pd

import btbphylo.utils as utils
import btbphylo.update_summary as update_summary
import btbphylo.consistify as consistify
import btbphylo.filter_samples as filter_samples
import btbphylo.phylogeny as phylogeny

DEFAULT_CLADE_INFO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CladeInfo.csv")

def update_samples(results_path, summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH):
    """
        Updates the local copy of the sample summary csv file containing metadata 
        for all samples file or builds a new one from scratch if it does not 
        already exist. Downloads all FinalOut.csv files from s3-csu-003 and appends 
        them to the a pandas DataFrame and saves the data to csv.

        Parameters:
            summary_filepath (str): path to location of summary csv  

        Returns:
            metadata (dict): metadata relating to the complete (unfiltered) dataset
    """
    print("\n## Update Summary ##\n")
    # create metadata path
    metadata_path = os.path.join(results_path, "metadata")
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    print("\tloading summary csv file ... \n")
    # download sample summary csv
    df_summary = update_summary.get_df_summary(summary_filepath)
    print("\tgetting s3 keys for batch summary files ... \n")
    # get s3 keys of FinalOut.csv for new batches of samples
    new_keys = update_summary.new_final_out_keys(df_summary)
    print("\tappending new metadata to df_summary ... \n")
    # update the summary dataframe
    updated_df_summary, metadata = update_summary.append_df_summary(df_summary, new_keys)
    print("\tsaving summary csv file ... \n")
    # save summary to csv 
    utils.df_to_csv(updated_df_summary, summary_filepath)
    # copy all_samples.csv to metadata
    shutil.copy(summary_filepath, os.path.join(metadata_path, "all_samples.csv"))
    return (metadata,)

def sample_filter(results_path, df_samples=None, allow_wipe_out=False, 
                  summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH, config=False, **kwargs):
    """ 
        Filters the sample summary csv file 

        Parameters:
            results_path (str): output path to results directory

            df_samples (pandas DataFrame): optional dataframe on which to filter.
            If not provided, the dataframe is parsed from summary_filepath csv.

            allow_wipe_out (bool): do not raise exception if 1 or fewer samples pass.

            summary_filepath (str): input path to location of summary csv  

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
            metadata (dict): filtering related metadata

            df_passed (pandas DataFrame object): a dataframe of 'Pass'
            only samples filtered according to criteria set out in 
            arguments.
    """
    print("\n## Filter Samples ##\n")
    # create metadatapath
    metadata_path = os.path.join(results_path, "metadata")
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    passed_filepath = os.path.join(metadata_path, "passed_samples.csv")
    # if no sample set provided
    if df_samples is None:
        print("\tremoving duplicates ... \n")
        # remove duplicate samples from full list of samples
        df_samples = filter_samples.remove_duplicates(utils.summary_csv_to_df(summary_filepath), 
                                                      pcMapped="max", Ncount="min")
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
    print("\tfiltering samples ... \n")
    # filter samples
    df_passed, metadata = filter_samples.get_samples_df(df_samples, allow_wipe_out, 
                                                        summary_filepath, **filter_args)
    print("\tsaving filtered samples csv ... \n")
    # save filtered_df to csv in metadata output folder
    utils.df_to_csv(df_passed, passed_filepath)
    # copy all_samples.csv to metadata
    shutil.copy(summary_filepath, os.path.join(metadata_path, "all_samples.csv"))
    return metadata, filter_args, df_passed, df_samples

def consistify_samples(results_path, cat_mov_path):
    """
        'Consistifies' wgs samples with cattle and movement samples; removes
        samples from each dataset that aren't present in all three datasets.
        Saves output .csvs to results_path.

        Parameters:
            results_path (str): output path to results directory

            cat_mov_path (str): path to folder containing cattle and movement
            .csv files 

        Returns:
            metadata (dict): metadata related to consitify

            wgs_consist (pandas DataFrame object): consistified wgs samples; contains
            the same fields as the summary csv 
    """
    print("\n## Consistify ##\n")
    # cattle and movement csv filepaths
    cattle_filepath = f"{cat_mov_path}/cattle.csv" 
    movement_filepath = f"{cat_mov_path}/movement.csv" 
    # validate paths
    if not os.path.exists(cattle_filepath):
        raise FileNotFoundError(f"Can't find cattle.csv in {cat_mov_path}")
    if not os.path.exists(movement_filepath):
        raise FileNotFoundError(f"Can't find movement.csv in {cat_mov_path}")
    metadata_path = os.path.join(results_path, "metadata")
    passed_filepath = os.path.join(metadata_path, "passed_samples.csv")
    # consistified file outpaths
    consistified_wgs_filepath = os.path.join(metadata_path, "consistified_wgs.csv")
    consistified_cattle_filepath = os.path.join(results_path, "cattle.csv")
    consistified_movement_filepath = os.path.join(results_path, "movement.csv")
    # run consistify and save metadata in results root
    print("\tconsistifying samples ... \n")
    metadata, wgs_consist = consistify.consistify_csvs(passed_filepath, cattle_filepath, movement_filepath,
                                                       consistified_wgs_filepath,  consistified_cattle_filepath, 
                                                       consistified_movement_filepath, metadata_path)
    # copy cattle and movement csvs to metadata
    shutil.copy(cattle_filepath, os.path.join(metadata_path, "cattle.csv"))
    shutil.copy(movement_filepath, os.path.join(metadata_path, "movement.csv"))
    return metadata, wgs_consist

def phylo(results_path, consensus_path, download_only=False, n_threads=1, 
          build_tree=False, df_passed=None, light_mode=False):
    """
        Runs phylogeny on filtered samples: Downloads consensus files, 
        concatonates into 1 large fasta file, runs snp-sites, runs snp-dists
        and runs megacc 

        Pramaters:
            results_path (str):  output path to results directory

            consenus_path (str): output path to directory for saving consensus files

            download_only (bool): only download consensus (do not run phylogeny)

            n_threads (int): number of threads for snp-dists

            build_tree (bool): build a phylogentic tree with megacc

            passed_filepath (str): optional input path to filtered samples csv

            filtered_df (pandas DataFrame object): optional dataframe containing 
            metadata for filtered samples
        
            light_mode (bool): If set to true multi_fasta.fas and snps.fas are
            saved to a temporary directory which is subsequently deleted 

            dash_c (bool): whether to run snp-sites with '-c'

        Returns:
            metadata (dict): phylogeny related metadata
    """
    metadata_path = os.path.join(results_path, "metadata")
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    metadata = {}
    # if df_passed DataFrame provided
    if df_passed is not None:
        pass
    # otherwise if consistified_wgs.csv in metadata folder: load csv
    elif os.path.exists(os.path.join(metadata_path, "consistified_wgs.csv")):
        df_passed = utils.summary_csv_to_df(os.path.join(metadata_path, "consistified_wgs.csv"))
    # otherwise if passed_samples.csv in metadata folder: load csv
    elif os.path.exists(os.path.join(metadata_path, "passed_samples.csv")):
        df_passed = utils.summary_csv_to_df(os.path.join(metadata_path, "passed_samples.csv"))
    else:
        raise ValueError("If passed_samples.csv does not exist in results_path ensure "
                         "that the filtered_df argument is provided")
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    # if light_mode: use temporary directory for fasta files
    if light_mode:
        fasta_path = tempfile.mkdtemp()
    # outherwise: save fastas to results directory
    else:
        fasta_path = results_path
    # output paths
    multi_fasta_path = os.path.join(fasta_path, "multi_fasta.fas")
    snp_sites_outpath = os.path.join(fasta_path, "snps.fas")
    snp_dists_outpath = os.path.join(results_path, "snps.csv")
    tree_path = os.path.join(results_path, "mega")
    print("\n## Phylogeny ##\n")
    # concatonate fasta files
    phylogeny.build_multi_fasta(multi_fasta_path, df_passed, consensus_path) 
    if not download_only:
        # run snp-sites
        print("\trunning snp_sites ... \n")
        metadata.update(phylogeny.snp_sites(snp_sites_outpath, multi_fasta_path))
        # run snp-dists
        print("\trunning snp_dists ... \n")
        phylogeny.build_snp_matrix(snp_dists_outpath, snp_sites_outpath, n_threads)
        if build_tree:
            if not os.path.exists(tree_path):
                os.makedirs(tree_path)        
            # build tree
            print("\n\trunning mega ... \n")
            phylogeny.build_tree(tree_path, snp_sites_outpath)
    if light_mode:
        shutil.rmtree(fasta_path)
    return (metadata,)

def full_pipeline(results_path, consensus_path, 
                  summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH, n_threads=1,
                  build_tree=False, download_only=False, cat_mov_path=None, **kwargs):
    """
        Updates local copy of summary csv file, filters samples and
        runs phylogeny 
    """
    # update full sample summary
    metadata_update, *_ = update_samples(results_path, summary_filepath)
    metadata = metadata_update
    # filter samples
    metadata_filt, filter_args, df_passed, _ = sample_filter(results_path, **kwargs)
    metadata.update(metadata_filt)
    # create metadatapath
    metadata_path = os.path.join(results_path, "metadata")
    # save filters to metadata output folder
    with open(os.path.join(metadata_path, "filters.json"), "w") as f:
        json.dump(filter_args, f, indent=2)
    # if running in ViewBovine must consistify datasets
    if cat_mov_path:
        # consistify datasets for ViewBovine
        metadata_consist, df_consistified = consistify_samples(results_path, cat_mov_path)
        # update metadata
        metadata.update(metadata_consist)
        # run phylogeny
        metadata_phylo, *_ = phylo(results_path, consensus_path, download_only, n_threads, 
                                   build_tree, df_passed=df_consistified, light_mode=True)
        if not download_only:
            # process sample names in snps.csv to be consistent with cattle and movement data
            phylogeny.post_process_snps_csv(os.path.join(results_path, "snps.csv"))
    else:
        # run phylogeny
        metadata_phylo, *_ = phylo(results_path, consensus_path, download_only, n_threads, 
                                   build_tree, df_passed=df_passed, light_mode=True)
    metadata.update(metadata_phylo)
    return (metadata,)

def report(df_no_dedup, df_passed, df_clade_info):
    # get dataframe of filtered samples
    df_filtered = df_no_dedup.drop(list(df_passed.index))
    df_report = df_filtered[["Submission", "Outcome", "flag"]]#, "pcMapped", "group", "Ncount"]]
    df_report["pcMapped"] = df_filtered["pcMapped"].map(lambda x: "Pass" if x >= 90 else "Fail")
    df_report["Ncount"] = "Fail"
    for clade, row in df_clade_info.iterrows():
        df_report["Ncount"][df_filtered.index[df_filtered["group"]==clade]] = \
            df_filtered.loc[df_filtered["group"]==clade].apply(lambda sample: "Pass" if sample["group"]==clade \
                and sample["Ncount"]<=row["maxN"] else "Fail", axis=1)
    return df_report

def view_bovine(results_path, consensus_path, cat_mov_path,  
                clade_info_path=DEFAULT_CLADE_INFO_PATH, 
                summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH):
    """
        Phylogeny for plugging into ViewBovine: filters samples with different 
        Ncount thresholds for each clade, consistifies samples with cattle and
        movement data, and runs phylogeny
    """
    # load CladeInfo.csv
    df_clade_info = pd.read_csv(clade_info_path, index_col="clade")
    # update full sample summary
    metadata_update, *_ = update_samples(results_path, summary_filepath)
    metadata = metadata_update
    # filter full set on pcMapped, flag and fail samples
    metadata_filt, filter_args, df_passed, df_no_dedup = sample_filter(results_path, flag=["BritishbTB"], 
                                                                       pcMapped=(90, 100))
    i = 1
    num_passed_samples = metadata_filt["number_of_passed_samples"]
    # loop through clades in CladeInfo.csv
    for clade, row in df_clade_info.iterrows():
        print(f"## Filtering samples for clade {i} / {len(df_clade_info)} ##")
        # filters samples within each clade according to Ncount in CladeInfo.csv
        metadata_filt, filter_clade, df_clade, _ = sample_filter(results_path, df_passed, allow_wipe_out=True, 
                                                                 group=[clade], Ncount=(0, row["maxN"]))
        del filter_clade["group"]
        filter_args[clade] = filter_clade
        # sum the number of filtered samples
        num_passed_samples += metadata_filt["number_of_passed_samples"]
        # update df_passed with cladewise filtering
        df_passed = pd.concat([df_passed, df_clade]).drop_duplicates(["Submission"])
        i += 1
    # create metadatapath
    metadata_path = os.path.join(results_path, "metadata")
    # save filters to metadata output folder
    with open(os.path.join(metadata_path, "filters.json"), "w") as f:
        json.dump(filter_args, f, indent=2)
    # overwrite passed_samples.csv in metadata output folder with updated df_passed
    utils.df_to_csv(df_passed, os.path.join(results_path, "metadata/passed_samples.csv"))
    # copy CladeInfo.csv into results folder
    metadata_path = os.path.join(results_path, "metadata")
    shutil.copy(clade_info_path, os.path.join(metadata_path, "CladeInfo.csv"))
    # update metadata
    metadata.update(metadata_filt)
    metadata["number_of_passed_samples"] = num_passed_samples
    # generate report
    df_report = report(df_no_dedup, df_passed, df_clade_info)
    df_report.to_csv("/home/nickpestell/tmp/test_3/report.csv", index=False)
    # consistify datasets for ViewBovine
    metadata_consist, df_consistified = consistify_samples(results_path, cat_mov_path)
    # update metadata
    metadata.update(metadata_consist)
    # run phylogeny
    metadata_phylo, *_ = phylo(results_path, consensus_path, n_threads=4, 
                               df_passed=df_consistified, light_mode=True)
    # process sample names in snps.csv to be consistent with cattle and movement data
    phylogeny.post_process_snps_csv(os.path.join(results_path, "snps.csv"))
    metadata.update(metadata_phylo)
    return (metadata,)

def parse_args():
    """
        Parse command line arguments for use with each function
    """
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
    subparser.add_argument("--clade", "-g", dest="group", nargs="+", help="optional filter")
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
    subparser.add_argument("cat_mov_path", help="if running for \
                           ViewBovine production provide a path to the folder containing cattle and movement .csv files")
    subparser.set_defaults(func=consistify_samples)

    # run phylogeny
    subparser = subparsers.add_parser('phylo', help='performs phylogeny')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("consensus_path", help="path to where consensus files will be held")
    subparser.add_argument("--download_only", help="if only dowloading connsensus sequences",
                           action="store_true", default=False)
    subparser.add_argument("--n_threads", "-j", default=1, help="number of threads for snp-dists")
    subparser.add_argument("--build_tree", action="store_true", default=False, help="build a tree")
    subparser.add_argument("--light_mode", action="store_true", default=False, help="save fastas to \
                           temporary directory")
    subparser.set_defaults(func=phylo)

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
    subparser.add_argument("--clade", "-g", dest="group", nargs="+", help="optional filter")
    subparser.add_argument("--outcome", dest="Outcome", nargs="+", help="optional filter")
    subparser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--genomecov", "-gc", dest="GenomeCov", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--n_count", "-nc", dest="Ncount", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--flag", "-f", dest="flag", nargs="+", help="optional filter")
    subparser.add_argument("--meandepth", "-md", dest="MeanDepth", type=float, nargs=2, help="optional filter")
    subparser.add_argument("--cat_mov_path", "-cmp", default=None, help="if running for \
                           ViewBovine production provide a path to the folder containing cattle and movement .csv files")
    subparser.set_defaults(func=full_pipeline)

    # view bovine
    subparser = subparsers.add_parser('ViewBovine', help="runs phylogeny with default settings for ViewBovine")
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("consensus_path", help = "path to where consensus files will be held")
    subparser.add_argument("cat_mov_path", help="if running for ViewBovine production provide a path to the \
                           folder containing cattle and movement .csv files")
    subparser.add_argument("--clade_info_path", help="path to CladeInfo csv file", 
                           default=DEFAULT_CLADE_INFO_PATH)
    subparser.add_argument("--summary_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_SUMMARY_FILEPATH)
    subparser.set_defaults(func=view_bovine)

    # pasre args
    kwargs = vars(parser.parse_args())
    if not kwargs:
       parser.print_help()
       sys.exit(0)
    return kwargs

def run(**kwargs):
    # metadata
    metadata = {"datetime": str(datetime.now())}
    btb_phylo_git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"])
    metadata["git_commit"] = btb_phylo_git_commit.decode().strip('\n')
    # retrieve opperation
    func = kwargs.pop("func")
    # run
    meta_update, *_ = func(**kwargs)
    # update metadata
    metadata.update(meta_update)
    # create metadata directory in results folder
    metadata_path = os.path.join(kwargs["results_path"], "metadata")
    if not os.path.exists(metadata_path):
        os.mkdir(metadata_path)
    # save metadata
    print("\nsaving metadata ... \n")
    meta_filepath = os.path.join(metadata_path, "metadata.json")
    with open(meta_filepath, "w") as f:
        json.dump(metadata, f, indent=2)
    print("Done!\n")


if __name__ == "__main__":
    # parse command line arguments
    kwargs = parse_args()
    # run btb-phylo with arguments
    run(**kwargs)
