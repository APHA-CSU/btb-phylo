
import pandas as pd

import btbphylo.utils as utils


"""
    Removes dupliacte WGS samples
"""

def remove_duplicates(df, **kwargs):
    """
        Drops duplicated submissions from df based on the names and values of kwargs.
        Duplicated samples are kept if their entry in the column specified by the 
        kwarg name contains either the min or max value (specified by the kwarg value). 
        If more than one sample in the column name contain the min or max value then 
        the second kwarg is used to choose which of these duplicates to keep. This 
        pattern continues for n kwargs. If no more kwargs exist, the duplicated sample 
        appearing first in the dataframe is chosen.

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv

            **kwargs: 1 (minimum) or more optional arguments. The names of the 
            arguments must be a column name in df, the values must be of type str equal 
            to either "min" or "max".    

        Returns:
            df (pandas DataFrame object)
    """
    if not kwargs:
        raise TypeError("no kwargs provided, provide a column name and method for "
                        "dropping duplicates, e.g. pcMapped='min'")
    # reamining indexes: starts as all indexes
    remaining_indexes = df.index
    for column_name, method in kwargs.items():
        if column_name not in df.columns:
            raise ValueError(f"Invalid kwarg '{column_name}': must be one of: " 
                             f"{', '.join(df.columns.to_list())}")
        if not pd.api.types.is_numeric_dtype(df[column_name]):
            raise utils.InvalidDtype(dtype="float or int", column_name=column_name)
        if method != "min" and method != "max":
            raise ValueError(f"Inavlid kwarg value: {column_name} must be either "
                             "'min' or 'max'")
        # get indexes to remove based on column_name and the selected method (min/max)
        indexes_to_remove = get_indexes_to_remove(df.loc[remaining_indexes], 
                                                  column_name, method)
        # update the remaining indexes by subtracting the indexes to remove
        remaining_indexes = remaining_indexes.difference(indexes_to_remove)
    # drop the indexes to remove
    return df.drop(df.index.difference(remaining_indexes)).drop_duplicates(["Submission"])

def get_indexes_to_remove(df, parameter, method):
    """
        Loops through unique submisions in the df and collects indexes for duplicate 
        submisions which should be excluded.

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv

            parameter (str): a column name from df on which to decide which 
            duplicate to keep, e.g. keep the submission with the largest pcMapped

            method (str): either 'min' or 'max', depending on whether the sample with
            the samllest or largest parameter value should be kept 

        Returns:
            indexes (pandas index object): indexes to remove from dataframe
    """
    indexes = pd.Index([])
    for submission_no in df.Submission.unique():
        if method == "max":
            threshold_1 = df.loc[df["Submission"]==submission_no][parameter].max()
        elif method == "min":
            threshold_1 = df.loc[df["Submission"]==submission_no][parameter].min()
        indexes = indexes.append(df.loc[(df["Submission"]==submission_no) & \
            (df[parameter] != threshold_1)].index)
    return indexes
