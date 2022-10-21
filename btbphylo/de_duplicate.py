
import pandas as pd


"""
    Removes dupliacte WGS samples
"""

def remove_duplicates(df, **kwargs):
    """
        Drops duplicated submissions from df based on the names and values of kwargs.
        Duplicated samples are kept if their entry in the column specified by the 
        kwarg name contains either the min or max value (specified by the kwarg value),
        for numerical columns. Or the value itself, for categorical columns. 
        If more than one sample in the column name contain the min or max value, or the
        value itself, then the second kwarg is used to choose which of these duplicates 
        to keep. This pattern continues for n kwargs. If no more kwargs exist, the 
        duplicated sample appearing first in the dataframe is chosen.

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv

            **kwargs: 1 (minimum) or more optional arguments. The names of the 
            arguments must be a column name in df, the values must be of type str equal 
            to either "min" or "max" if the specified column is numeric, or a value
            within the DataFrame's column if the specified column is categorical.    

        Returns:
            df (pandas DataFrame object)
    """
    if not kwargs:
        raise TypeError("no kwargs provided, provide a column name and value for "
                        "dropping duplicates, e.g. pcMapped='min'")
    # reamining indexes: starts as all indexes
    remaining_indexes = df.index
    for column_name, value in kwargs.items():
        if column_name not in df.columns:
            raise ValueError(f"Invalid kwarg '{column_name}': must be one of: " 
                             f"{', '.join(df.columns.to_list())}")
        if pd.api.types.is_numeric_dtype(df[column_name]):
            if value != "min" and value != "max":
                raise ValueError(f"Inavlid kwarg value: '{value}', for numerical column, "
                                "must be either 'min' or 'max'")
        elif value not in list(df[column_name]):
            raise ValueError(f"Inavlid kwarg value: '{value}', for categorical column, "
                             f"must be a value in the '{column_name}' column")
        # get indexes to remove based on column_name and the selected value (min/max)
        indexes_to_remove = get_indexes_to_remove(df.loc[remaining_indexes], 
                                                  column_name, value)
        # update the remaining indexes by subtracting the indexes to remove
        remaining_indexes = remaining_indexes.difference(indexes_to_remove)
    # drop the indexes to remove - additional .drop_duplicates ensures the first appearing
    # is kept if not resolved
    return df.drop(df.index.difference(remaining_indexes)).drop_duplicates(["Submission"])

def get_indexes_to_remove(df, parameter, method):
    """
        Loops through unique submisions in the df and collects indexes for duplicate 
        submisions which should be excluded.

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv

            parameter (str): a column name from df on which to decide which 
            duplicate to keep, e.g. keep the submission with the largest pcMapped

            method (str): A value on which to filter the parameter. Either 'min' 
            or 'max' for numerical columns, depending on whether the sample with
            the samllest or largest parameter value should be kept. For categorical
            columns, method should be once of the possible values in df for that 
            column 

        Returns:
            indexes (pandas index object): indexes to remove from dataframe
    """
    indexes = pd.Index([])
    submission_list = list(df["Submission"])
    for submission_no in df.Submission.unique():
        # ensure that only duplicates entries are considered
        if submission_list.count(submission_no) > 1: 
            # if parameter is numeric: set the threshold value to the max or min value 
            # of that paramater for all samples with a Submission number of submission_no
            if pd.api.types.is_numeric_dtype(df[parameter]):
                if method == "max":
                    threshold = df.loc[df["Submission"]==submission_no][parameter].max()
                elif method == "min":
                    threshold = df.loc[df["Submission"]==submission_no][parameter].min()
            # otherwise: set the threshold to the method parameter
            elif pd.api.types.is_categorical_dtype(df[parameter]) or \
                    pd.api.types.is_object_dtype(df[parameter]):
                threshold = method 
            # find all indexes for samples with Submission number = submission_no, where
            # the parameter is not equal to threshold
            indexes = indexes.append(df.loc[(df["Submission"]==submission_no) & \
                (df[parameter] != threshold)].index)
    return indexes
