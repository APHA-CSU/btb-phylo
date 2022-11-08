import argparse
import os
import json

import pandas as pd

import btb_phylo

"""
    Script for generating clade-wise Ncount thresholds for filtering 
    samples for ViewBovine. Saves a 'CladeInfo.csv', containing the 
    thresholds for each clade and a 'metadta/metadata.json' to the
    results directory.
"""

def run(results_path, perc):
    # update samples
    metadata,  df_all_wgs = btb_phylo.update_samples(results_path)
    # get pass-only
    metadata_filt, _, df_wgs_passed, _ = btb_phylo.sample_filter(results_path,
                                                                 df_all_wgs)
    metadata.update(metadata_filt)
    # remove duplicates
    metadata_dedup, df_wgs_deduped = \
        btb_phylo.de_duplicate_samples(results_path,
                                       df_wgs_passed,
                                       Outcome="Pass", 
                                       flag="BritishbTB", 
                                       pcMapped="max", 
                                       Ncount="min")
    metadata.update(metadata_dedup)
    # get Ncount thresholds
    df_cladeinfo = pd.DataFrame(df_wgs_deduped.groupby("group")["Ncount"].\
        quantile(perc))
    df_cladeinfo.index.names = ["clade"]
    df_cladeinfo.rename(columns = {"Ncount":"maxN"}, inplace=True)
    df_cladeinfo.drop(["MicPin", "Microti", "Pinnipedii", "bTB", "nonbTB"], 
                      inplace=True)
    df_cladeinfo["maxN"] = df_cladeinfo["maxN"].round()
    # save csv
    df_cladeinfo.to_csv(os.path.join(results_path, "CladeInfo.csv"))
    # save metadata
    meta_filepath = os.path.join(results_path, "metadata/metadata.json")
    with open(meta_filepath, "w") as f:
        json.dump(metadata, f, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="max_n")
    parser.add_argument("results_path", help="output path to results directory")
    parser.add_argument("--perc", default=0.85, help="the percentage cutoff")
    args = parser.parse_args()
    run(args.results_path, args.perc)
