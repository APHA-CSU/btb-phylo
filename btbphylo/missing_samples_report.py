import pandas as pd

"""
    Generates a Pandas DataFrame which reports on all samples that are excluded 
    from ViewBovine. 
"""

def get_excluded(df_deduped, df_included):
    """
        Returns a DataFrame for WGS samples that are filtered. The DataFrame is 
        in the form of df_summary (see Readme). 
    """
    # get dataframe of excluded samples: df_deduped - df_included
    return df_deduped[~df_deduped["Submission"].isin(df_included["Submission"])]

def exclusion_reason(df_excluded, df_clade_info):
    """
        Returns a "report" DataFrame, detailing the reasons why each WGS sample
        is excluded.
    """
    # subsample df_excluded columns
    df_report = df_excluded[["Submission", "Outcome", "flag"]].copy()
    # map pcMapped column to a pass/fail value
    df_report["pcMapped"] = df_excluded["pcMapped"]\
        .map(lambda x: "Pass" if x >= 90 else "Fail")
    # map Ncount column to a pass/fail value
    df_report["Ncount"] = "Fail"
    for clade, row in df_clade_info.iterrows():
        df_report["Ncount"][df_excluded.index[df_excluded["group"]==clade]] = \
            df_excluded.loc[df_excluded["group"]==clade].\
                apply(lambda sample: "Pass" if sample["Ncount"]<=row["maxN"] \
                    else "Fail", axis=1)
    return df_report

def missing_data(df_report, missing_wgs_samples, missing_cattle_samples, 
                 missing_movement_samples):
    """
        Appends columns to the report DataFrame, detailing whether each sample
        is present in WGS, cattle and movement datasets. 
    """
    df_report_missing_data = df_report.copy()
    # append columns for indicating pressence of WGS, cattle and movement data
    # for each sample
    df_report_missing_data["wgs_data"] = True
    df_report_missing_data["cattle_data"] = None 
    df_report_missing_data["movement_data"] = None 
    # generate report df for samples with missing wgs data
    df_report_missing_wgs = pd.DataFrame({"Submission": list(missing_wgs_samples), 
                                          "Outcome": [None]*len(missing_wgs_samples),
                                          "flag": [None]*len(missing_wgs_samples),
                                          "pcMapped": [None]*len(missing_wgs_samples),
                                          "Ncount": [None]*len(missing_wgs_samples),
                                          "wgs_data": [False]*len(missing_wgs_samples),
                                          "cattle_data": [None]*len(missing_wgs_samples),
                                          "movement_data": [None]*len(missing_wgs_samples)})
    # append missing wgs samples to df_report
    df_report_missing_data = pd.concat((df_report_missing_data, 
                                        df_report_missing_wgs), ignore_index=True)
    # fill the cattle_data and movement data columns with True/False indicating 
    # the prescence of each sample in the given dataset
    df_report_missing_data["cattle_data"] = df_report_missing_data["Submission"].\
        map(lambda x: x not in missing_cattle_samples)
    df_report_missing_data["movement_data"] = df_report_missing_data["Submission"].\
        map(lambda x: x not in missing_movement_samples)
    return df_report_missing_data

def report(df_deduped, df_included, missing_wgs_samples, 
           missing_cattle_samples, missing_movement_samples, df_clade_info):
    """
        Generates a Pandas DataFrame which reports on all samples that are 
        excluded from ViewBovine. The DataFrame has an entry for each 
        excluded sample and columns for all possible reasons of exclusion, 
        including filtering: Outcome, flag, pcMapped, Ncount. It also 
        includes True/False values for each samples prescence in the 3 
        required datasets: WGS, cattle and movement.  

        Parameters:
            df_deduped (pandas DataFrame object): WGS samples with duplicates 
            removed. In the form of df_summary

            df_included (pandas DataFrame object): WGS samples included in 
            ViewBovine. In the form of df_summary

            df_clade_info (pandas DataFrame object): Ncount filters for each
            WGS clade

            missing_wgs_samples (set): set of sample names missing from WGS
            data

            missing_cattle_samples (set): set of sample names missing from 
            cattle data

            missing_movement_samples (set): set of sample names missing from 
            movement data

        Returns:
            report (pandas DataFrame object): report of samples excluded from
            ViewBovine
    """
    return df_deduped.pipe(get_excluded, df_included).pipe(exclusion_reason, df_clade_info).\
        pipe(missing_data, missing_wgs_samples, missing_cattle_samples, 
             missing_movement_samples)