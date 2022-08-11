import pandas as pd

import btbphylo.utils as utils

"""
    Ensure ViewBovine datasets are consistent by dropping the samples 
    that don't appear in every file
"""

# TODO: unit test
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
    wgs_consist = wgs.loc[wgs["Submission"].isin(consist_samples)]
    cattle_consist = cattle[cattle.CVLRef.isin(consist_samples)]
    movements_consist = movements[movements.SampleName.isin(consist_samples)]
    # metadata
    metadata = {"original_number_of_wgs_records": len(wgs),
                "original_number_of_cattle_records": len(cattle),
                "original_number_of_movement_records": len(movements),
                "consistified_number_of_wgs_records": len(wgs_consist),
                "consistified_number_of_cattle_records": len(cattle_consist),
                "consistified_number_of_movement_records": len(movements_consist)}
    return metadata, wgs_consist, cattle_consist, movements_consist,\
        missing_wgs, missing_cattle, missing_movement

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
    cattle = pd.read_csv(cattle_path, dtype={"Cphh":"Int64", "TestDate":object, "CVLRef":object, 
                                             "RefYr":"Int64", "MapX":"Int64", "MapY":"Int64", "RawEartag2":object}) 
    movements = pd.read_csv(movement_path, dtype={"AnimalId":"Int64", "BirthCPH":object, "StandardEartag":object, 
                                                  "ImportDate":object, "DeathDate":object, "SampleName":object, 
                                                  "Cphh":"Int64", "Cult":object, "BreakId":object, "CPH":object,
                                                  "TestDate":object, "Birth":"Int64", "Death":"Int64", 
                                                  "MovementDate":object, "MovementId":"Int64", 
                                                  "OffLocation":"Int64", "OnLocation":"Int64", "Stay_Length": "Int64", 
                                                  "offLocationKey":"Int64", "offMapRef":object, 
                                                  "offPostCode":object, "offX":"Int64", "offY":"Int64",
                                                  "onLocationKey":"Int64", "onMapRef":object, "onPostCode":object,
                                                  "onX":"Int64", "onY":"Int64", "vLocationID":"Int64", 
                                                  "LocationName":object, "LocationId":object}) 
    # consistify
    (metadata, wgs_consist, cattle_consist, movements_consist, missing_wgs, \
        missing_cattle, missing_movement) = consistify(wgs, cattle, movements)
    # save consistified csvs
    utils.df_to_csv(wgs_consist, consistified_wgs_path)
    cattle_consist.to_csv(consistified_cattle_path, index=False)
    movements_consist.to_csv(consisitified_movements_path, index=False)
    # save missing samples csvs
    (pd.DataFrame(missing_wgs)).to_csv(missing_samples_path + "/missing_snps.csv", index=False)
    (pd.DataFrame(missing_cattle)).to_csv(missing_samples_path + "/missing_cattle.csv", index=False)
    (pd.DataFrame(missing_movement)).to_csv(missing_samples_path + "/missing_movement.csv", index=False)
    return metadata, wgs_consist
