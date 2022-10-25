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
import btbphylo.de_duplicate as de_duplicate
import btbphylo.consistify as consistify
import btbphylo.missing_samples_report as missing_samples_report
import btbphylo.filter_samples as filter_samples
import btbphylo.phylogeny as phylogeny

DEFAULT_CLADE_INFO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CladeInfo.csv")

def update_samples(results_path, all_wgs_samples_filepath=utils.DEFAULT_WGS_SAMPLES_FILEPATH):
    """
        Updates the local copy of the 'all_wgs_samples' .csv file containing WGS metadata 
        for all WGS samples. Or builds a new file from scratch if it does not 
        already exist. Downloads all FinalOut.csv files from s3-csu-003 and appends 
        them to the a pandas DataFrame and saves the data to csv.

        Parameters:
            results_path (str): output path to results directory

            all_wgs_samples_filepath (str): path to location of summary csv  

        Returns:
            metadata (dict): metadata relating to the complete (unfiltered) dataset

            df_all_wgs_updated (pandas DataFrame object): updated dataframe 
            containing all WGS samples held in s3-csu-003.
    """
    print("\n## Update Summary ##\n")
    # create metadata path
    metadata_path = os.path.join(results_path, "metadata")
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    print("\tloading summary csv file ... \n")
    # download sample summary csv
    df_all_wgs = update_summary.get_df_summary(all_wgs_samples_filepath)
    print("\tgetting s3 keys for batch summary files ... \n")
    # get s3 keys of FinalOut.csv for new batches of samples
    new_keys = update_summary.new_final_out_keys(df_all_wgs)
    print("\tappending new metadata to df_summary ... \n")
    # update the summary dataframe
    df_all_wgs_updated, metadata = update_summary.append_df_summary(df_all_wgs, new_keys)
    print("\tsaving summary csv file ... \n")
    # save summary to csv 
    utils.df_to_csv(df_all_wgs, all_wgs_samples_filepath)
    # copy all_wgs_samples.csv to metadata
    shutil.copy(all_wgs_samples_filepath, os.path.join(metadata_path, "all_wgs_samples.csv"))
    return metadata, df_all_wgs_updated

def de_duplicate_samples(results_path, df_wgs_samples=None, 
                         all_wgs_samples_filepath=utils.DEFAULT_WGS_SAMPLES_FILEPATH, **kwargs):
    """
        'De-duplicates' WGS samples in df_wgs_samples. Removes dupliacte entries from WGS 
        data based based on key value pairs in kwargs. If df_samples is not provided,
        all_wgs_samples_filepath csv is parsed and used. Automatically saves the de_uplicated
        samples to 'deduped_wgs.csv' in the results metadata folder.

        Parameters:
            results_path (str): output path to results directory

            df_samples (pandas DataFrame object): WGS samples to de-duplicate. 
            DataFrame is of the same form as df_summary and all_wgs_samples.csv.
            
            all_wgs_samples_filepath (str): path to location of summary csv  

        Returns:
            metadata (dict): metadata relating to the complete (unfiltered) dataset

            df_deduped (pandas DataFrame object): a deduplicated version of 
            df_samples

    """
    print("\n## De-Duplicate ##\n")
    # create metadatapath
    metadata_path = os.path.join(results_path, "metadata")
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    # remove unused kwargs
    args = {k: v for k, v in kwargs.items() if v is not None}
    # load df_samples from summary csv if dataframe not provided
    if df_wgs_samples is None:
        df_wgs_samples = utils.summary_csv_to_df(all_wgs_samples_filepath)
    # remove duplicates
    metadata, df_wgs_deduped = de_duplicate.remove_duplicates(df_wgs_samples, **args) 
    # save deduped wgs to metadata path
    metadata_path = os.path.join(results_path, "metadata")
    df_wgs_deduped.to_csv(os.path.join(metadata_path, "deduped_wgs.csv"), index=False)
    # copy all_wgs_samples.csv to metadata
    shutil.copy(all_wgs_samples_filepath, os.path.join(metadata_path, "all_wgs_samples.csv"))
    return metadata, df_wgs_deduped

def consistify_samples(results_path, cattle_movements_path, df_wgs_samples=None,
                       all_wgs_samples_filepath=utils.DEFAULT_WGS_SAMPLES_FILEPATH):
    """
        'Consistifies' WGS samples with cattle and movement samples; removes
        samples from each dataset that aren't present in all three datasets.
        Automatically saves th output .csvs to the results_path.

        Parameters:
            results_path (str): output path to results directory

            cattle_movements_path (str): path to folder containing cattle and 
            movement .csv files 

            df_wgs_samples (pandas DataFrame): optional dataframe on which to 
            consistify. If not provided, the dataframe is parsed from 
            all_wgs_samples_filepath csv.

            all_wgs_samples_filepath (str): input path to location of summary csv  

        Returns:
            metadata (dict): metadata related to consitify

            missing_wgs (set): samples that are missing from WGS data

            missing_cattle (set): samples that are missing from cattle 
            data

            missing movement (set): samples that are missing from 
            movement data
            
            df_wgs_consist (pandas DataFrame object): consistified wgs samples; 
            contains the same fields as the summary csv 
    """
    print("\n## Consistify ##\n")
    # cattle and movement csv filepaths
    cattle_filepath = f"{cattle_movements_path}/cattle.csv" 
    movement_filepath = f"{cattle_movements_path}/movement.csv" 
    # validate paths
    if not os.path.exists(cattle_filepath):
        raise FileNotFoundError(f"Can't find cattle.csv in {cattle_movements_path}")
    if not os.path.exists(movement_filepath):
        raise FileNotFoundError(f"Can't find movement.csv in {cattle_movements_path}")
    # create metadatapath
    metadata_path = os.path.join(results_path, "metadata")
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    # consistified file outpaths
    consistified_wgs_filepath = os.path.join(metadata_path, "consistified_wgs.csv")
    consistified_cattle_filepath = os.path.join(results_path, "cattle.csv")
    consistified_movement_filepath = os.path.join(results_path, "movement.csv")
    # load
    if df_wgs_samples is None:
        df_wgs_samples = utils.summary_csv_to_df(all_wgs_samples_filepath)
    df_cattle_samples = pd.read_csv(cattle_filepath, dtype=object)
    df_movement_samples = pd.read_csv(movement_filepath, dtype=object)
    # process data
    print("\tconsistifying samples ... \n")
    metadata, df_wgs_consist, df_cattle_corrected, df_movement_fixed =\
            consistify.process_datasets(df_wgs_samples, df_cattle_samples, 
                                        df_movement_samples)
    # save consistified cattle & movement csvs
    utils.df_to_csv(df_wgs_consist, consistified_wgs_filepath)
    df_cattle_corrected.to_csv(consistified_cattle_filepath, index=False)
    df_movement_fixed.to_csv(consistified_movement_filepath, index=False)
    # copy cattle and movement csvs to metadata
    shutil.copy(cattle_filepath, os.path.join(metadata_path, "cattle.csv"))
    shutil.copy(movement_filepath, os.path.join(metadata_path, "movement.csv"))
    # copy all_wgs_samples.csv to metadata
    shutil.copy(all_wgs_samples_filepath, os.path.join(metadata_path, "all_wgs_samples.csv"))
    return metadata, df_wgs_consist

def sample_filter(results_path, df_wgs_samples=None, allow_wipe_out=False, 
                  all_wgs_samples_filepath=utils.DEFAULT_WGS_SAMPLES_FILEPATH, config=False, **kwargs):
    """ 
        Filters the WGS samples. Automatically saves the the filtered csv file to 
        'passed_samples.csv' in the results metadata folder.

        Parameters:
            results_path (str): output path to results directory

            df_wgs_samples (pandas DataFrame): optional dataframe on which to filter.
            If not provided, the dataframe is parsed from all_wgs_samples_filepath csv.

            allow_wipe_out (bool): do not raise exception if 1 or fewer samples pass.

            all_wgs_samples_filepath (str): input path to location of summary csv  

            config (str): path to location of config json file

            **kwargs: 0 or more optional arguments. Names must match a 
            column name in all_wgs_samples.csv. If column is of type 
            'categorical' or 'object', vales must be of type 'list' 
            ispecifying a set of values to match against the argument 
            name's column in btb_wgs_samples.csv. For example, 
            'sample_name=["AFT-61-03769-21", "20-0620719"]' will include 
            just these two samples. If column is of type 'int' or 'float',
            values must be of type 'tuple' and of length 2, specifying a 
            min and max value for that column. 

        Returns:
            metadata (dict): filtering related metadata

            filter_args (dict): args used for filtering

            df_wgs_passed (pandas DataFrame object): a dataframe of 'Pass'
            only samples filtered according to criteria set out in 
            arguments

            df_wgs_samples (pandas DataFrame object): see parameters
    """
    print("\n## Filter Samples ##\n")
    # create metadatapath
    metadata_path = os.path.join(results_path, "metadata")
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    # if no sample set provided
    if df_wgs_samples is None:
        df_wgs_samples = utils.summary_csv_to_df(all_wgs_samples_filepath) 
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
    df_wgs_passed, metadata = filter_samples.get_samples_df(df_wgs_samples, allow_wipe_out, 
                                                            all_wgs_samples_filepath, **filter_args)
    print("\tsaving filtered samples csv ... \n")
    # save filtered_df to csv in metadata output folder
    utils.df_to_csv(df_wgs_passed, os.path.join(metadata_path, "wgs_passed_samples.csv"))
    # copy all_wgs_samples.csv to metadata
    shutil.copy(all_wgs_samples_filepath, os.path.join(metadata_path, "all_wgs_samples.csv"))
    return metadata, filter_args, df_wgs_passed, df_wgs_samples

def phylo(results_path, consensus_path, download_only=False, n_threads=1, 
          build_tree=False, df_wgs=None, light_mode=False):
    """
        Runs phylogeny on WGS samples: Downloads consensus files, 
        concatonates into 1 large fasta file, runs snp-sites, runs snp-dists
        and runs megacc. Automatically writes the results on-disk (see parameters). 

        Pramaters:
            results_path (str):  output path to results directory

            consenus_path (str): output path to directory for saving consensus files

            download_only (bool): only download consensus (do not run phylogeny)

            n_threads (int): number of threads for snp-dists

            build_tree (bool): build a phylogentic tree with megacc

            df_wgs (pandas DataFrame object): wgs samples on which to perform phylogeny

            filtered_df (pandas DataFrame object): optional dataframe containing 
            metadata for filtered samples
        
            light_mode (bool): If set to true multi_fasta.fas and snps.fas are
            saved to a temporary directory which is subsequently deleted 

            dash_c (bool): whether to run snp-sites with '-c'

        Returns:
            metadata (dict): phylogeny related metadata
    """
    # create metadatapath
    metadata_path = os.path.join(results_path, "metadata")
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    metadata = {}
    # if df_passed DataFrame provided
    if df_wgs is not None:
        pass
    # otherwise if consistified_wgs.csv in metadata folder: load csv
    elif os.path.exists(os.path.join(metadata_path, "consistified_wgs.csv")):
        df_wgs = utils.summary_csv_to_df(os.path.join(metadata_path, "consistified_wgs.csv"))
    # otherwise if passed_samples.csv in metadata folder: load csv
    elif os.path.exists(os.path.join(metadata_path, "passed_samples.csv")):
        df_wgs = utils.summary_csv_to_df(os.path.join(metadata_path, "wgs_passed_samples.csv"))
    else:
        raise ValueError("If wgs_passed_samples.csv does not exist in results_path ensure "
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
    phylogeny.build_multi_fasta(multi_fasta_path, df_wgs, consensus_path) 
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
                  all_wgs_samples_filepath=utils.DEFAULT_WGS_SAMPLES_FILEPATH, n_threads=1,
                  build_tree=False, download_only=False, **kwargs):
    """
        Runs the full pipeline: 
            1. updates with new WGS samples;
            2. removes duplicated WGS samples;
            3. filters WGS samples;
            4. runs phylogeny
        Saves all results and metadata to results_path

        Pramaters:
            results_path (str):  output path to results directory

            consenus_path (str): output path to directory for saving consensus files

            all_wgs_samples_filepath (str): input path to location of summary csv  

            n_threads (int): the number of threads to use for building the snp_matrix

            build_tree (bool): build a phylogentic tree using mega

            download_only (bool): only download consensus files without running 
                phylogeny

            **kwargs: see sample_filter() for available kwargs
        
        Returns:
            metadata (dict): full_pipeline metadata
    """
    # update full sample summary
    metadata_update, df_all_wgs = update_samples(results_path, all_wgs_samples_filepath)
    metadata = metadata_update
    # remove duplicates
    metadata_dedup, df_wgs_deduped = de_duplicate_samples(results_path,
                                                          df_wgs_samples=df_all_wgs,
                                                          Outcome="Pass", 
                                                          flag="BritishbTB", 
                                                          pcMapped="max", 
                                                          Ncount="min")
    metadata.update(metadata_dedup)
    # filter samples
    metadata_filt, filter_args, df_wgs_passed, _ = sample_filter(results_path, df_wgs_deduped, **kwargs)
    metadata.update(metadata_filt)
    # save filters to metadata output folder
    metadata_path = os.path.join(results_path, "metadata")
    with open(os.path.join(metadata_path, "filters.json"), "w") as f:
        json.dump(filter_args, f, indent=2)
    # run phylogeny
    metadata_phylo, *_ = phylo(results_path, consensus_path, download_only, n_threads, 
                               build_tree, df_wgs_passed, light_mode=True)
    metadata.update(metadata_phylo)
    return (metadata,)

def view_bovine(results_path, consensus_path, cattle_movements_path,  
                clade_info_path=DEFAULT_CLADE_INFO_PATH, 
                all_wgs_samples_filepath=utils.DEFAULT_WGS_SAMPLES_FILEPATH,
                **kwargs):
    """
        Phylogeny for plugging into ViewBovine: 
            1. updates with new WGS samples;
            2. removes duplicated WGS samples;
            3. filters WGS samples with different Ncount thresholds for each clade; 
            4. consistifies WGS samples with cattle and movement data;
            5. generates a report of missing sampes;
            6. runs phylogeny;
            7. post-processes snp-matrix to have consistent names with cattle and
                movement data.
        Saves all results and metadata to results_path

        Pramaters:
            results_path (str):  output path to results directory

            consenus_path (str): output path to directory for saving consensus files

            cattle_movements_path (str): path to folder containing cattle and movement
                .csv files 
            
            clade_info_path (str): path to CladeInfo csv file 

            all_wgs_samples_filepath (str): input path to location of summary csv  

            **kwargs: see sample_filter() for available kwargs
        
        Returns:
            metadata (dict): ViewBovine metadata
    """
    # create metadatapath
    metadata_path = os.path.join(results_path, "metadata")
    # load CladeInfo.csv
    df_clade_info = pd.read_csv(clade_info_path, index_col="clade")
    # update full sample summary
    metadata_update, df_all_wgs = update_samples(results_path, all_wgs_samples_filepath)
    metadata = metadata_update
    # remove duplicates
    metadata_dedup, df_wgs_deduped = de_duplicate_samples(results_path,
                                                          df_wgs_samples=df_all_wgs,
                                                          Outcome="Pass", 
                                                          flag="BritishbTB", 
                                                          pcMapped="max", 
                                                          Ncount="min")
    metadata.update(metadata_dedup)
    df_wgs_passed = pd.DataFrame(columns=["Sample", "GenomeCov", "MeanDepth", 
                                         "NumRawReads", "pcMapped", "Outcome", 
                                         "flag", "group", "CSSTested", "matches", 
                                         "mismatches", "noCoverage", "anomalous",
                                         "Ncount", "ResultLoc", "ID", "TotalReads", 
                                         "Abundance", "Submission"])
    i = 1
    num_passed_samples = 0
    filter_args = {}
    # loop through clades in CladeInfo.csv
    for clade, row in df_clade_info.iterrows():
        print(f"## Filtering samples for clade {i} / {len(df_clade_info)} ##")
        # filters samples within each clade according to Ncount in CladeInfo.csv
        metadata_filt, filter_clade, df_clade, _ = sample_filter(results_path, 
                                                                 df_wgs_deduped, 
                                                                 allow_wipe_out=True, 
                                                                 group=[clade],
                                                                 Ncount=(0, row["maxN"]),
                                                                 **kwargs)
        del filter_clade["group"]
        filter_args[clade] = filter_clade
        # sum the number of filtered samples
        num_passed_samples += metadata_filt["number_of_passed_samples"]
        # update df_passed with cladewise filtering
        df_wgs_passed = pd.concat([df_wgs_passed, df_clade])
        i += 1
    # save filtered_df to csv in metadata output folder
    utils.df_to_csv(df_wgs_passed, os.path.join(metadata_path, "passed_samples.csv"))
    # save filters to metadata output folder
    with open(os.path.join(metadata_path, "filters.json"), "w") as f:
        json.dump(filter_args, f, indent=2)
    # copy CladeInfo.csv into results folder
    shutil.copy(clade_info_path, os.path.join(metadata_path, "CladeInfo.csv"))
    # update metadata
    metadata.update(metadata_filt)
    metadata["number_of_passed_samples"] = num_passed_samples
    # consistify datasets
    metadata_consist, df_wgs_consistified = consistify_samples(results_path, 
                                                              cattle_movements_path,
                                                              df_wgs_samples=df_wgs_passed)
    # update metadata
    metadata.update(metadata_consist)
    # generate report of missing samples
    df_report = missing_samples_report.report(df_wgs_deduped, df_wgs_consistified, cattle_movements_path, 
                                              df_clade_info)
    # save report to metadata folder
    df_report.to_csv(os.path.join(metadata_path, "report.csv"), index=False)
    # run phylogeny
    metadata_phylo, *_ = phylo(results_path, consensus_path, n_threads=4, 
                               df_wgs=df_wgs_consistified, light_mode=True)
    # process sample names in the snp matrix: snps.csv to be consistent with cattle 
    # and movement data
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
    subparser.add_argument("--all_wgs_samples_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_WGS_SAMPLES_FILEPATH)
    subparser.set_defaults(func=update_samples)

    # filter samples
    subparser = subparsers.add_parser('filter', help='filters wgs_samples.csv file')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("--all_wgs_samples_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_WGS_SAMPLES_FILEPATH)
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

    # de_duplicate
    subparser = subparsers.add_parser('de_duplicate', help='removes duplicated wgs samples from wgs_samples.csv')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("--all_wgs_samples_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_WGS_SAMPLES_FILEPATH)
    subparser.add_argument("--outcome", dest="Outcome", help="optional filter, must be a valid value in Outcome column \
                           of wgs_samples csv")
    subparser.add_argument("--flag", "-f", dest="flag", help="optional filter, must be a valid value in flag column \
                           of wgs_samples csv")
    subparser.add_argument("--genomecov", "-gc", dest="GenomeCov", help="optional filter, must be 'min' or 'max'")
    subparser.add_argument("--n_count", "-nc", dest="Ncount", help="optional filter, must be 'min' or 'max'")
    subparser.add_argument("--pcmapped", "-pc", dest="pcMapped", help="optional filter, must be 'min' or 'max'")
    subparser.add_argument("--meandepth", "-md", dest="MeanDepth", help="optional filter, must be 'min' or 'max'")
    subparser.set_defaults(func=de_duplicate_samples)

    # consistify
    subparser = subparsers.add_parser('consistify', help='removes wgs samples that are missing from \
                                      cattle and movement data (metadata warehouse)')
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("cattle_movements_path", help="if running for \
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
    subparser.add_argument("--all_wgs_samples_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_WGS_SAMPLES_FILEPATH)
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
    subparser.set_defaults(func=full_pipeline)

    # view bovine
    subparser = subparsers.add_parser('ViewBovine', help="runs phylogeny with default settings for ViewBovine")
    subparser.add_argument("results_path", help="path to results directory")
    subparser.add_argument("consensus_path", help = "path to where consensus files will be held")
    subparser.add_argument("cattle_movements_path", help="if running for ViewBovine production provide a path to the \
                           folder containing cattle and movement .csv files")
    subparser.add_argument("--clade_info_path", help="path to CladeInfo csv file", 
                           default=DEFAULT_CLADE_INFO_PATH)
    subparser.add_argument("--all_wgs_samples_filepath", help="path to sample metadata .csv file", 
                           default=utils.DEFAULT_WGS_SAMPLES_FILEPATH)
    subparser.add_argument("--pcmapped", "-pc", dest="pcMapped", type=float, nargs=2, help="optional filter",
                           default=(90, 100))
    subparser.add_argument("--flag", "-f", dest="flag", nargs="+", help="optional filter", default=["BritishbTB"])
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
