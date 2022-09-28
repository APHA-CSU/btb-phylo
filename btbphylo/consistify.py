import pandas as pd

import btbphylo.utils as utils

"""
    Ensure ViewBovine datasets are consistent by dropping the samples 
    that don't appear in every file
"""

def consistify(wgs, cattle, movements):
    """
        Consistifies the wgs, cattle and movement datasets, i.e. removes 
        samples from each dataset which are not common to all three.

        Parameters:
            wgs (pandas DataFrame object): read from the 
            filtered_samples.csv output from filter_samples.py 

            cattle (pandas DataFrame object): cattle data from the 
            metadata warehouse

            movements (pandas Dataframe object): movement data from 
            the metadata warehouse

        Returns:
            wgs_consist (pandas DataFrame object): consistified wgs

            cattle_consist (pandas DataFrame object): consistified
            cattle

            movement_consist (pandas DataFrame object): consistified
            movement

            missing_wgs (pandas DataFrame object): wgs samples which are 
            not common to both cattle and movement

            missing_cattle (pandas DataFrame object): cattle samples which
            are not common to both wgs and movement

            missing movement (pandas DataFrame object): movement samples
            which are not common to both wgs and cattle
    """
    # sets of sample names for the different datasets
    wgs_samples = set(wgs.Submission)
    cattle_samples = set(cattle.CVLRef)
    movement_samples = set(movements.SampleName)
    # subsample to select common sample names
    consist_samples = wgs_samples.intersection(cattle_samples)\
        .intersection(movement_samples)
    # extract samples from dataset not common to all three
    missing_wgs = pd.DataFrame({"Submission": list(wgs_samples - consist_samples)})
    missing_cattle = pd.DataFrame({"CVLRef": list(cattle_samples - consist_samples)})
    missing_movement = pd.DataFrame({"SampleName": list(movement_samples - consist_samples)})
    # subsample full datasets by common names
    wgs_consist = wgs.loc[wgs["Submission"].isin(consist_samples)].copy()
    cattle_consist = cattle[cattle.CVLRef.isin(consist_samples)].copy()
    movements_consist = movements[movements.SampleName.isin(consist_samples)].copy()
    return wgs_consist, cattle_consist, movements_consist,\
        missing_wgs, missing_cattle, missing_movement

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
def fix_movements(movements):
    """
        removes NaNs from Stay_Length column to avoid error in ViewBovine
    """
    return movements[movements['Stay_Length'].notna()]

def process_datasets(wgs, cattle, movements):
    """
        Fully processes the datasets so that they are prepped for ViewBovine. This involves
        consistifying, fixing clade mismatches in cattle data and removing movement data 
        entries where "Stay_Length" = NaN. These two latter features are to avoid errors
        when using the ViewBovine app.
    """
    # consistify datasets
    wgs_consist, cattle_consist, movements_consist, missing_wgs, missing_cattle, \
        missing_movements = consistify(wgs.copy(), cattle.copy(), movements.copy())
    # correct clade assignment in cattle csv
    cattle_corrected = clade_correction(wgs_consist, cattle_consist)
    # fix movement data
    fixed_movements = fix_movements(movements_consist)
    # metadata
    metadata = {"original_number_of_wgs_records": len(wgs),
                "original_number_of_cattle_records": len(cattle),
                "original_number_of_movement_records": len(movements),
                "consistified_number_of_wgs_records": len(wgs_consist),
                "consistified_number_of_cattle_records": len(cattle_corrected),
                "consistified_number_of_movement_records": len(movements_consist)}
    return metadata, wgs_consist, cattle_corrected, fixed_movements, \
        missing_wgs, missing_cattle, missing_movements

def consistify_csvs(filtered_samples_path, cattle_path, movement_path, 
                    consistified_wgs_path, consistified_cattle_path, 
                    consisitified_movements_path, missing_samples_path):
    """
        An I/O layer for consistify: Parses wgs, cattle and movement CSVs.
        Runs consistify().
        Saves consistified outputs to CSV
    """
    # load
    wgs = utils.summary_csv_to_df(filtered_samples_path)
    cattle = pd.read_csv(cattle_path, dtype=object)
    movements = pd.read_csv(movement_path, dtype=object)
    # process data
    (metadata, wgs_consist, cattle_corrected, movements_fixed, missing_wgs, \
        missing_cattle, missing_movement) = process_datasets(wgs, cattle, movements)
    # save consistified csvs
    utils.df_to_csv(wgs_consist, consistified_wgs_path)
    cattle_corrected.to_csv(consistified_cattle_path, index=False)
    movements_fixed.to_csv(consisitified_movements_path, index=False)
    # save missing samples csvs
    (pd.DataFrame(missing_wgs)).to_csv(missing_samples_path + "/missing_snps.csv", index=False)
    (pd.DataFrame(missing_cattle)).to_csv(missing_samples_path + "/missing_cattle.csv", index=False)
    (pd.DataFrame(missing_movement)).to_csv(missing_samples_path + "/missing_movement.csv", index=False)
    return metadata, wgs_consist
