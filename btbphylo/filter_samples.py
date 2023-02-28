import warnings
import re

import pandas as pd

import btbphylo.utils as utils

"""
    Filters WGS samples according to criteria based on the fields in
    summary csv file
"""

warnings.formatwarning = utils.format_warning


def filter_df(df, allow_wipe_out=False, **kwargs):
    """ 
        Filters WGS df (which is based off 'all_wgs_samples' csv file) 
        according to a set of criteria. 

        Parameters:
            df (pandas DataFrame object): a dataframe read from 
            all_wgs_samples csv file.

            allow_wipe_out (bool): do not raise exception if 1 or fewer 
            samples pass.

            **kwargs: 0 or more optional arguments. Names must match a 
            column name in btb_wgs_samples.csv. If column is of type 
            'categorical' or 'object', vales must be of type 'list' 
            specifying a set of values to match against the argument 
            name's column in btb_wgs_samples.csv. For example, 
            'sample_name=["AFT-61-03769-21", "20-0620719"]' will include 
            just these two samples. If column is of type 'int' or 
            'float', values must be of type 'tuple' and of length 2, 
            specifying a min and max value for that column. 

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
        if pd.api.types.is_categorical_dtype(df[key]) or \
                pd.api.types.is_object_dtype(df[key]):
            # add categorical columns in **kwargs to categorical_kwargs
            categorical_kwargs[key] = kwargs[key]
        else:
            # add numerical columns in **kwargs to numerical_kwargs
            numerical_kwargs[key] = kwargs[key]
    # calls filter_columns_catergorical() with **categorical_kwargs on df, pipes
    # the output into filter_columns_numeric() with **numerical_kwargs and 
    # assigns the output to a new df_passed
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
                raise utils.InvalidDtype(dtype="float or int", 
                                         column_name=column_name)
            # ensures that values are of length 2 (min & max) and numeric
            if (not isinstance(value, list) and not isinstance(value,tuple)) or\
                len(value) != 2 or (not isinstance(value[0], float) and not \
                    isinstance(value[0], int)) \
                        or (not isinstance(value[1], float) and not \
                            isinstance(value[1], int)) or value[0] >= value[1]:
                raise ValueError(f"Invalid kwarg '{column_name}': must be list \
                    or tuple of 2 numbers where the 2nd element is larger than \
                        the 1st")
        # constructs a query string on which to query df; e.g. 'pcMapped >= 90 
        # and pcMapped <= 100 and GenomeCov >= 80 and GenomveCov <= 100'
        query = ' and '.join(f'{col} >= {vals[0]} and {col} <= {vals[1]}' \
            for col, vals in kwargs.items())
        return df.query(query)
    else:
        return df

def filter_columns_categorical(df, **kwargs):
    """ 
        Filters the summary dataframe according to kwargs, where keys
        are the columns on which to filter and the values are lists 
        containing the values of df[kwarg[key]] to retain. 
    """ 
    for column_name, value in kwargs.items():
        # ensures that column_names are of type object or categorical
        if not (pd.api.types.is_categorical_dtype(df[column_name.strip("not_")]) \
                or pd.api.types.is_object_dtype(df[column_name.strip("not_")])):
            raise utils.InvalidDtype(dtype="category or object", 
                                     column_name=column_name)
        # ensures that values are list of strings
        if not isinstance(value, list) or not all(isinstance(item, str) for \
            item in value):
            raise ValueError(f"Invalid kwarg '{column_name}': must be a list \
                of strings")
        # issues a warning if any value is missing from specified column
        if not re.match(r'~', column_name):
            missing_values = \
                [item for item in value if item not in list(df[column_name])]
            if missing_values:
                warnings.warn(f"Column '{column_name}' does not contain the values "
                            f"'{', '.join(missing_values)}'")
    # constructs a query string on which to query df; e.g. 'Outcome in [Pass] 
    # and sample_name in ["AFT-61-03769-21", "20-0620719"]. 
    query = ' and '.join([(f'{col} not in {vals}' if re.match(r'not_', col) \
                           else (f'{col} in {vals}')) for \
                            col, vals in kwargs.items()])
    return df.query(query)

def get_wgs_samples_df(df_samples=None, allow_wipe_out=False, 
                       summary_filepath=utils.DEFAULT_WGS_SAMPLES_FILEPATH, 
                       **kwargs):
    """
        Gets all the WGS samples to be included in phylogeny. Parses 
        all_wgs_samples csv file into a pandas DataFrame. Filters the 
        DataFrame arcording to criteria descriped in **kwargs. 
    """
    # pipes the output DataFrame from wgs_csv_to_df() (all wgs samples) into 
    # filter_df() i.e. wgs_csv_to_df() | filter_df() > df
    if df_samples is not None:
        df = df_samples.pipe(filter_df, allow_wipe_out, **kwargs)
    else:
        df = utils.wgs_csv_to_df(summary_filepath).pipe(filter_df, 
                                                        allow_wipe_out,
                                                        **kwargs)
    metadata = {"number_of_passed_samples": len(df)}
    return df, metadata
