import pandas as pd

import btbphylo.utils as utils

"""
    Ensure ViewBovine datasets are consistent by dropping the samples 
    that don't appear in every file
"""

def consistify(df_wgs, df_cattle, df_movement):
    """
        Consistifies the WGS, cattle and movement datasets, i.e. removes 
        samples from each dataset which are not common to all three.

        Parameters:
            df_wgs (pandas DataFrame object): read from the 
            filtered_samples.csv output from filter_samples.py 

            df_cattle (pandas DataFrame object): cattle data from the 
            metadata warehouse

            df_movement (pandas Dataframe object): movement data from 
            the metadata warehouse

        Returns:
            df_wgs_consist (pandas DataFrame object): consistified wgs

            df_cattle_consist (pandas DataFrame object): consistified
            cattle

            df_movement_consist (pandas DataFrame object): consistified
            movement

            missing_wgs (set): samples that are missing from WGS data

            missing_cattle (set): samples that are missing from cattle 
            data

            missing movement (set): samples that are missing from 
            movement data
    """
    # sets of sample names for the different datasets
    wgs_samples = set(df_wgs.Submission)
    cattle_samples = set(df_cattle.CVLRef)
    movement_samples = set(df_movement.SampleName)
    # subsample to select common sample names
    consist_samples = wgs_samples.intersection(cattle_samples)\
        .intersection(movement_samples)
    # extract the missing samples
    missing_wgs_samples = (cattle_samples | movement_samples)-wgs_samples
    missing_cattle_samples = (wgs_samples | movement_samples)-cattle_samples
    missing_movement_samples = (wgs_samples | cattle_samples)-movement_samples
    # subsample full datasets by common names
    df_wgs_consist = \
        df_wgs.loc[df_wgs["Submission"].isin(consist_samples)].copy()
    df_cattle_consist = df_cattle[df_cattle.CVLRef.isin(consist_samples)].copy()
    df_movement_consist = \
        df_movement[df_movement.SampleName.isin(consist_samples)].copy()
    return df_wgs_consist, df_cattle_consist, df_movement_consist,\
        missing_wgs_samples, missing_cattle_samples, missing_movement_samples

def clade_correction(df_wgs, df_cattle):
    """
        Ensures that the clade assigment in cattle csv matches the clade 
        in WGS data. Assumes wgs clade is correct and overwrites the 
        cattle calde if there is a mismatch. This feature corrects an 
        error where the wrong clade is assigned in the MDWH.
    """
    df_cattle_corrected = df_cattle.copy()
    for _, row in df_cattle_corrected.iterrows():
        row["clade"] = \
            df_wgs.loc[df_wgs["Submission"]==row["CVLRef"], "group"].iloc[0]
    return df_cattle_corrected

def process_datasets(df_wgs, df_cattle, df_movement):
    """
        Fully processes the datasets so that they are prepped for 
        ViewBovine. This involves consistifying, fixing clade mismatches
        in cattle data and removing movement data entries where 
        "Stay_Length" = NaN. These two latter features are to avoid 
        errors when using the ViewBovine app.
    """
    # consistify datasets
    df_wgs_consist, df_cattle_consist, df_movement_consist, *_ =\
        consistify(df_wgs.copy(), df_cattle.copy(), df_movement.copy())
    # correct clade assignment in cattle csv
    df_cattle_corrected = clade_correction(df_wgs_consist, df_cattle_consist)
    # metadata
    metadata = {"original_number_of_wgs_records": len(df_wgs),
                "original_number_of_cattle_records": len(df_cattle),
                "original_number_of_movement_records": len(df_movement),
                "consistified_number_of_wgs_records": len(df_wgs_consist),
                "consistified_number_of_cattle_records": \
                    len(df_cattle_corrected),
                "consistified_number_of_movement_records": \
                    len(df_movement_consist)}
    return metadata, df_wgs_consist, df_cattle_corrected, df_movement_consist

def consistify_csvs(wgs_samples_path, cattle_path, movement_path, 
                    consistified_wgs_path, consistified_cattle_path, 
                    consisitified_movement_path):
    """
        An I/O layer for consistify: Parses wgs, cattle and movement 
        CSVs. Runs consistify(). Saves consistified outputs to CSV
    """
    # load
    df_wgs = utils.wgs_csv_to_df(wgs_samples_path)
    df_cattle = pd.read_csv(cattle_path, dtype=object)
    df_movement = pd.read_csv(movement_path, dtype=object)
    # process data
    metadata, df_wgs_consist, df_cattle_corrected, df_movement_fixed, \
        missing_wgs_samples, missing_cattle_samples, missing_movement_samples =\
            process_datasets(df_wgs, df_cattle, df_movement)
    # save consistified csvs
    utils.df_to_csv(df_wgs_consist, consistified_wgs_path)
    df_cattle_corrected.to_csv(consistified_cattle_path, index=False)
    df_movement_fixed.to_csv(consisitified_movement_path, index=False)
    return metadata, missing_wgs_samples, missing_cattle_samples, \
        missing_movement_samples, df_wgs_consist
