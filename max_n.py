import argparse
import os
import json

import pandas as pd

import btb_phylo


def run(results_path, perc):
    (metadata,) = btb_phylo.update_samples(results_path)
    (metadata_filt, df_filtered) = btb_phylo.sample_filter(results_path)
    metadata.update(metadata_filt)
    df_cladeinfo = pd.DataFrame(df_filtered.groupby("group")["Ncount"].quantile(perc))
    df_cladeinfo.index.names = ["clade"]
    df_cladeinfo.rename(columns = {"Ncount":"maxN"}, inplace=True)
    df_cladeinfo.to_csv(os.path.join(results_path, "CladeInfo.csv"))
    meta_filepath = os.path.join(results_path, "metadata/metadata.json")
    with open(meta_filepath, "w") as f:
        json.dump(metadata, f, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="max_n")
    parser.add_argument("results_path")
    parser.add_argument("perc")
    args = parser.parse_args()
    run(args.results_path, args.perc)

