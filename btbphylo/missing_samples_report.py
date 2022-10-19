import pandas as pd

def report(df_no_dedup, df_included, missing_wgs_samples, missing_cattle_samples, 
           missing_movement_samples, df_clade_info):
    # get dataframe of excluded samples: df_deduped - df_included
    df_excluded = df_no_dedup[~df_no_dedup["Submission"].isin(df_included["Submission"])]
    # subsample columns
    df_report = df_excluded[["Submission", "Outcome", "flag"]]
    df_report["pcMapped"] = df_excluded["pcMapped"].map(lambda x: "Pass" if x >= 90 else "Fail")
    df_report["Ncount"] = "Fail"
    for clade, row in df_clade_info.iterrows():
        df_report["Ncount"][df_excluded.index[df_excluded["group"]==clade]] = \
            df_excluded.loc[df_excluded["group"]==clade].apply(lambda sample: "Pass" if sample["group"]==clade \
                and sample["Ncount"]<=row["maxN"] else "Fail", axis=1)
    df_report["wgs_data"] = True
    df_report["cattle_data"] = None 
    df_report["movement_data"] = None 
    df_missing_wgs = pd.DataFrame({"Submission": list(missing_wgs_samples), 
                                   "Outcome": [None]*len(missing_wgs_samples),
                                   "flag": [None]*len(missing_wgs_samples),
                                   "pcMapped": [None]*len(missing_wgs_samples),
                                   "Ncount": [None]*len(missing_wgs_samples),
                                   "wgs_data": [False]*len(missing_wgs_samples),
                                   "cattle_data": [None]*len(missing_wgs_samples),
                                   "movement_data": [None]*len(missing_wgs_samples)})
    df_report = pd.concat((df_report, df_missing_wgs), ignore_index=True)
    df_report["cattle_data"] = df_report["Submission"].map(lambda x: x not in missing_cattle_samples)
    df_report["movement_data"] = df_report["Submission"].map(lambda x: x not in missing_movement_samples)
    return df_report