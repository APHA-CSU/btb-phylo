import pandas as pd

from btbphylo.consistify import consistify

"""
    Generates a Pandas DataFrame which reports on all samples that are excluded 
    from ViewBovine. 
"""

def get_excluded(df_wgs_deduped, df_wgs_included):
    """
        Returns a DataFrame for WGS samples that are filtered. The DataFrame is 
        in the form of df_summary (see Readme). 
    """
    # get dataframe of excluded samples: df_deduped - df_included
    return df_wgs_deduped[~df_wgs_deduped["Submission"].isin(df_wgs_included["Submission"])]

def exclusion_reason(df_wgs_excluded, df_clade_info):
    """
        Returns a "report" DataFrame, detailing the reasons why each WGS sample
        is excluded.
    """
    # subsample df_excluded columns
    df_report = df_wgs_excluded[["Submission", "Outcome", "flag"]].copy()
    # map pcMapped column to a pass/fail value
    df_report["pcMapped"] = df_wgs_excluded["pcMapped"]\
        .map(lambda x: "Pass" if x >= 90 else "Fail")
    # map Ncount column to a pass/fail value
    df_report["Ncount"] = "Fail"
    for clade, row in df_clade_info.iterrows():
        mask = df_wgs_excluded.index[df_wgs_excluded["group"]==clade]
        df_report.loc[mask, "Ncount"] = df_wgs_excluded.loc[df_wgs_excluded["group"]==clade].\
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

def add_eartag_column(df_report, df_cattle, df_movement):
    """
        Add an eartag column. Eartag comes from cattle or movement data.
        If the submission is missing from either cattle or movement data
        the entry is None.
    """
    # setup new dataframe with eartag column in the 2nd position
    df_report_eartag = df_report.copy()
    df_report_eartag.insert(loc=1, column="eartag", value=None)
    # map df_cattle['RawEartag2'] to df_report_eartag['eartag'] if there is 
    # cattle data. Else, map df_movement['StandardEartag'] to 
    # df_report_eartag['eartag'] if there is movement data. Else value is None.
    df_report_eartag["eartag"] = df_report.apply(lambda x: \
        df_cattle.loc[df_cattle["CVLRef"]==x["Submission"],"RawEartag2"].item()\
            if x["cattle_data"] else df_movement.loc[df_movement["SampleName"] \
                ==x["Submission"],"StandardEartag"].item() if \
                    x["movement_data"] else None, axis=1)
    return df_report_eartag

def report(df_wgs_deduped, df_wgs_included, cattle_movements_path, df_clade_info):
    """
        Generates a Pandas DataFrame which reports on all samples that are 
        excluded from ViewBovine. The DataFrame has an entry for each 
        excluded sample and columns for all possible reasons of exclusion, 
        including filtering: Outcome, flag, pcMapped, Ncount. It also 
        includes True/False values for each sample's prescence in the 3 
        required datasets: WGS, cattle and movement.  

        Parameters:
            df_wgs_deduped (pandas DataFrame object): WGS samples with duplicates 
            removed. In the form of df_summary

            df_wgs_included (pandas DataFrame object): WGS samples included in 
            ViewBovine. In the form of df_summary

            cattle_movements_path (str): path to folder containing cattle and 
            movement .csv files 

            df_clade_info (pandas DataFrame object): Ncount filters for each
            WGS clade

        Returns:
            report (pandas DataFrame object): report of samples excluded from
            ViewBovine
    """
    # cattle and movement csv filepaths
    cattle_filepath = f"{cattle_movements_path}/cattle.csv" 
    movement_filepath = f"{cattle_movements_path}/movement.csv" 
    df_cattle = pd.read_csv(cattle_filepath, dtype=object)
    df_movement = pd.read_csv(movement_filepath, dtype=object)
    # get missing wgs, cattle and movement samples
    _, _, _, missing_wgs_samples, missing_cattle_samples, missing_movement_samples = \
        consistify(df_wgs_deduped, df_cattle, df_movement)
    # process data to return the full report
    return df_wgs_deduped.pipe(get_excluded, df_wgs_included).pipe(exclusion_reason, df_clade_info).\
        pipe(missing_data, missing_wgs_samples, missing_cattle_samples, 
             missing_movement_samples).pipe(add_eartag_column, df_cattle, 
                                            df_movement)