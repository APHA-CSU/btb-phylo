import warnings

import pandas as pd

import btbphylo.utils as utils

"""
    Filters the samples according to criteria based on the fields in
    summary csv file
"""

warnings.formatwarning = utils.format_warning


class InvalidDtype(Exception):
    def __init__(self, message="Invalid series name. Series must be of correct type", 
                 *args, **kwargs):
        super().__init__(message, args, kwargs)
        if "dtype" in kwargs:
            self.message = f"Invalid series name. Series must be of type {kwargs['dtype']}"
        if "column_name" in kwargs:
            self.message = f"Invalid series name '{kwargs['column_name']}'. Series must be of the correct type"
        if "column_name" in kwargs and "dtype" in kwargs:
            self.message = f"Invalid series name '{kwargs['column_name']}'. Series must be of type {kwargs['dtype']}"
        else:
            self.message = message

    def __str__(self):
        return self.message

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
            raise InvalidDtype(dtype="float or int", column_name=column_name)
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

def filter_df(df, allow_wipe_out=False, **kwargs):
    """ 
        Filters the sample summary dataframe which is based off 
        btb_wgs_samples.csv according to a set of criteria. 

        Parameters:
            df (pandas DataFrame object): a dataframe read from btb_wgs_samples.csv.

            allow_wipe_out (bool): do not raise exception if 1 or fewer samples pass.

            **kwargs: 0 or more optional arguments. Names must match a 
            column name in btb_wgs_samples.csv. If column is of type 
            'categorical' or 'object', vales must be of type 'list' 
            ispecifying a set of values to match against the argument 
            name's column in btb_wgs_samples.csv. For example, 
            'sample_name=["AFT-61-03769-21", "20-0620719"]' will include 
            just these two samples. If column is of type 'int' or 'float',
            values must be of type 'tuple' and of length 2, specifying a 
            min and max value for that column. 

        Returns:
            df_passed (pandas DataFrame object): a dataframe of 'Pass'
            only samples filtered according to criteria set out in 
            arguments.

            ValueError: if any kwarg is not in df.columns  
    """
    # add "Pass" only samples and pcmap_theshold to the filtering 
    # criteria by default
    if "Outcome" not in kwargs:
        categorical_kwargs = {"Outcome": ["Pass"]}
    else:
        categorical_kwargs = {}
    numerical_kwargs = {}
    for key in kwargs.keys():
        if key not in df.columns:
            raise ValueError(f"Invalid kwarg '{key}': must be one of: " 
                             f"{', '.join(df.columns.to_list())}")
        else:
            if pd.api.types.is_categorical_dtype(df[key]) or \
                    pd.api.types.is_object_dtype(df[key]):
                # add categorical columns in **kwargs to categorical_kwargs
                categorical_kwargs[key] = kwargs[key]
            else:
                # add numerical columns in **kwargs to numerical_kwargs
                numerical_kwargs[key] = kwargs[key]
    # calls filter_columns_catergorical() with **categorical_kwargs on df, pipes 
    # the output into filter_columns_numeric() with **numerical_kwargs and assigns
    # the output to a new df_passed
    df_passed = df.pipe(filter_columns_categorical, 
                            **categorical_kwargs).pipe(filter_columns_numeric, 
                                                        **numerical_kwargs)
    if not allow_wipe_out and len(df_passed) < 2:
        raise Exception("1 or fewer samples meet specified criteria")
    return df_passed
    
def filter_columns_numeric(df, **kwargs):
    """ 
        Filters the summary dataframe according to kwargs, where keys
        are the columns on which to filter and the values must be tuple 
        of length 2 with min and max thresholds in elements 0 and 1. 
        The data in column name must me of dtype int or float. 
    """ 
    if kwargs:
        for column_name, value in kwargs.items():
            # ensures that column_names are of numeric type
            if not pd.api.types.is_numeric_dtype(df[column_name]):
                raise InvalidDtype(dtype="float or int", column_name=column_name)
            # ensures that values are of length 2 (min & max) and numeric
            if (not isinstance(value, list) and not isinstance(value,tuple)) or len(value) != 2 \
                or (not isinstance(value[0], float) and not isinstance(value[0], int)) \
                or (not isinstance(value[1], float) and not isinstance(value[1], int)) \
                or value[0] >= value[1]:
                raise ValueError(f"Invalid kwarg '{column_name}': must be list or tuple of 2" 
                                " numbers where the 2nd element is larger than the 1st")
        # constructs a query string on which to query df; e.g. 'pcMapped >= 90 and 
        # pcMapped <= 100 and GenomeCov >= 80 and GenomveCov <= 100'
        query = ' and '.join(f'{col} >= {vals[0]} and {col} <= {vals[1]}' \
            for col, vals in kwargs.items())
        return df.query(query)
    else:
        return df

def filter_columns_categorical(df, **kwargs):
    """ 
        Filters the summary dataframe according to kwargs, where keys
        are the columns on which to filter and the values are lists containing
        the values of df[kwarg[key]] to retain. 
    """ 
    for column_name, value in kwargs.items():
        # ensures that column_names are of type object or categorical
        if not (pd.api.types.is_categorical_dtype(df[column_name]) or \
            pd.api.types.is_object_dtype(df[column_name])):
            raise InvalidDtype(dtype="category or object", column_name=column_name)
        # ensures that values are list of strings
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError(f"Invalid kwarg '{column_name}': must be a list of strings")
        # issues a warning if any value is missing from specified column
        missing_values = [item for item in value if item not in list(df[column_name])]
        if missing_values:
            warnings.warn(f"Column '{column_name}' does not contain the values "
                          f"'{', '.join(missing_values)}'")
    # constructs a query string on which to query df; e.g. 'Outcome in [Pass] and 
    # sample_name in ["AFT-61-03769-21", "20-0620719"]. 
    query = ' and '.join(f'{col} in {vals}' for col, vals in kwargs.items())
    return df.query(query)

def get_samples_df(df_samples=None, summary_filepath=utils.DEFAULT_SUMMARY_FILEPATH, **kwargs):
    """
        Gets all the samples to be included in phylogeny. Loads btb_wgs_samples.csv
        into a pandas DataFrame. Filters the DataFrame arcording to criteria descriped in
        **kwargs. Removes Duplicated submissions.
    """
    # pipes the output DataFrame from summary_csv_to_df() (all samples) into filter_df()
    # into remove duplicates()
    # i.e. summary_csv_to_df() | filter_df() | remove_duplicates() > df
    if df_samples is not None:
        df = df_samples.pipe(filter_df, **kwargs).pipe(remove_duplicates, pcMapped="max", Ncount="min")
    else:
        df = utils.summary_csv_to_df(summary_filepath).pipe(filter_df,
                                                            **kwargs).pipe(remove_duplicates, 
                                                                        pcMapped="max", Ncount="min")
    metadata = {"number_of_passed_samples": len(df)}
    return df, metadata
