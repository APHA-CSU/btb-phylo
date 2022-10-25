import pandas as pd

import btbphylo.utils as utils

"""
    Ensure ViewBovine datasets are consistent by dropping the samples 
    that don't appear in every file
"""

def consistify(wgs, cattle, movement):
    """
        Consistifies the wgs, cattle and movement datasets, i.e. removes 
        samples from each dataset which are not common to all three.

        Parameters:
            wgs (pandas DataFrame object): read from the 
            filtered_samples.csv output from filter_samples.py 

            cattle (pandas DataFrame object): cattle data from the 
            metadata warehouse

            movement (pandas Dataframe object): movement data from 
            the metadata warehouse

        Returns:
            wgs_consist (pandas DataFrame object): consistified wgs

            cattle_consist (pandas DataFrame object): consistified
            cattle

            movement_consist (pandas DataFrame object): consistified
            movement

            missing_wgs (set): samples that are missing from WGS data

            missing_cattle (set): samples that are missing from cattle 
            data

            missing movement (set): samples that are missing from 
            movement data
    """
    # sets of sample names for the different datasets
    wgs_samples = set(wgs.Submission)
    cattle_samples = set(cattle.CVLRef)
    movement_samples = set(movement.SampleName)
    # subsample to select common sample names
    consist_samples = wgs_samples.intersection(cattle_samples)\
        .intersection(movement_samples)
    # extract the missing samples
    missing_wgs_samples = (cattle_samples | movement_samples)-wgs_samples
    missing_cattle_samples = (wgs_samples | movement_samples)-cattle_samples
    missing_movement_samples = (wgs_samples | cattle_samples)-movement_samples
    # subsample full datasets by common names
    wgs_consist = wgs.loc[wgs["Submission"].isin(consist_samples)].copy()
    cattle_consist = cattle[cattle.CVLRef.isin(consist_samples)].copy()
    movement_consist = movement[movement.SampleName.isin(consist_samples)].copy()
    return wgs_consist, cattle_consist, movement_consist,\
        missing_wgs_samples, missing_cattle_samples, missing_movement_samples

def clade_correction(wgs, cattle):
    """
        Ensures that the clade assigment in cattle csv matches the clade in
        WGS data. Assumes wgs clade is correct and overwrites the cattle calde
        if there is a mismatch. This feature corrects an error where the wrong 
        clade is assigned in the MDWH.
    """
    cattle_corrected = cattle.copy()
    for _, row in cattle_corrected.iterrows():
        row["clade"] = wgs.loc[wgs["Submission"]==row["CVLRef"], "group"].iloc[0]
    return cattle_corrected

# TODO: move this feature into sql scripts in ViewBovine repo
def fix_movement(movement):
    """
        removes NaNs from Stay_Length column to avoid error in ViewBovine
    """
    return movement[movement['Stay_Length'].notna()]

def process_datasets(wgs, cattle, movement):
    """
        Fully processes the datasets so that they are prepped for ViewBovine. This involves
        consistifying, fixing clade mismatches in cattle data and removing movement data 
        entries where "Stay_Length" = NaN. These two latter features are to avoid errors
        when using the ViewBovine app.
    """
    # consistify datasets
    wgs_consist, cattle_consist, movement_consist, *_ =\
        consistify(wgs.copy(), cattle.copy(), movement.copy())
    # correct clade assignment in cattle csv
    cattle_corrected = clade_correction(wgs_consist, cattle_consist)
    # fix movement data
    fixed_movement = fix_movement(movement_consist)
    # metadata
    metadata = {"original_number_of_wgs_records": len(wgs),
                "original_number_of_cattle_records": len(cattle),
                "original_number_of_movement_records": len(movement),
                "consistified_number_of_wgs_records": len(wgs_consist),
                "consistified_number_of_cattle_records": len(cattle_corrected),
                "consistified_number_of_movement_records": len(movement_consist)}
    return metadata, wgs_consist, cattle_corrected, fixed_movement

def consistify_csvs(wgs_samples_path, cattle_path, movement_path, 
                    consistified_wgs_path, consistified_cattle_path, 
                    consisitified_movement_path):
    """
        An I/O layer for consistify: Parses wgs, cattle and movement CSVs.
        Runs consistify().
        Saves consistified outputs to CSV
    """
    # load
    wgs = utils.summary_csv_to_df(wgs_samples_path)
    cattle = pd.read_csv(cattle_path, dtype=object)
    movement = pd.read_csv(movement_path, dtype=object)
    # process data
    metadata, wgs_consist, cattle_corrected, movement_fixed, missing_wgs_samples, \
        missing_cattle_samples, missing_movement_samples =\
            process_datasets(wgs, cattle, movement)
    # save consistified csvs
    utils.df_to_csv(wgs_consist, consistified_wgs_path)
    cattle_corrected.to_csv(consistified_cattle_path, index=False)
    movement_fixed.to_csv(consisitified_movement_path, index=False)
    return metadata, missing_wgs_samples, missing_cattle_samples, \
        missing_movement_samples, wgs_consist
