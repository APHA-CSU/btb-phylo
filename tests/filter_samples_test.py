import unittest

import pandas as pd
import numpy.testing as nptesting

from btbphylo import filter_samples


class TestFilterSamples(unittest.TestCase):
    def test_filter_df(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A": pd.Series(["a", "b", "c", "d", "e"], dtype="object"),
                                "Outcome": pd.Series(["Fail", "Pass", "Pass", "Pass", "Pass"], dtype="category"),
                                "pcMapped": pd.Series([0.1, 0.2, 0.3, 0.4, 0.5], dtype=float),
                                "column_D": pd.Series([1, 3, 5, 7, 9], dtype=int)})
        # test individual filters
        outcome = filter_samples.filter_df(test_df, pcMapped=(0.1, 0.3))
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A": ["b", "c"], "Outcome": ["Pass", "Pass"],
                                     "pcMapped": [0.2, 0.3], "column_D": [3, 5]}).values)
        outcome = filter_samples.filter_df(test_df, column_A=["a", "b", "e"])
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A": ["b", "e"], "Outcome": ["Pass", "Pass"],
                                     "pcMapped": [0.2, 0.5], "column_D": [3, 9]}).values)
        outcome = filter_samples.filter_df(test_df, column_D=(7, 10))
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A": ["d", "e"], "Outcome": ["Pass", "Pass"],
                                     "pcMapped": [0.4, 0.5], "column_D": [7, 9]}).values)
        # test multiple filters
        outcome = filter_samples.filter_df(test_df, pcMapped=(0.15, 0.45),
                                           column_A=["a", "b", "d", "e"], column_D=(2, 8))
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A": ["b", "d"], "Outcome": ["Pass", "Pass"],
                                     "pcMapped": [0.2, 0.4], "column_D": [3, 7]}).values)
        outcome = filter_samples.filter_df(test_df, pcMapped=(0.05, 0.45),
                                           column_A=["a", "b", "d", "e"], column_D=(0.5, 8), Outcome=["Pass", "Fail"])
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A": ["a", "b", "d"], "Outcome": ["Fail", "Pass", "Pass"],
                                     "pcMapped": [0.1, 0.2, 0.4], "column_D": [1, 3, 7]}).values)
        # test no filters
        outcome = filter_samples.filter_df(test_df)
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A": pd.Series(["b", "c", "d", "e"], dtype="object"),
                                                                   "Outcome": pd.Series(["Pass", "Pass", "Pass", "Pass"], dtype="category"),
                                                                   "pcMapped": pd.Series([0.2, 0.3, 0.4, 0.5], dtype=float),
                                                                   "column_D": pd.Series([3, 5, 7, 9], dtype=int)}).values)
        # test empty output < 1 samples
        # empty
        with self.assertRaises(Exception):
            filter_samples.filter_df(test_df, pcMapped=(0.15, 0.16),
                                     column_A=["b", "c"], column_D=(2, 3))
        # 1 sample
        with self.assertRaises(Exception):
            filter_samples.filter_df(test_df, pcMapped=(0.15, 0.25),
                                     column_A=["b", "c"], column_D=(2, 4))
        # test exception is not raised with allow_wipe_out=True
        # empty
        filter_samples.filter_df(test_df, allow_wipe_out=True, pcMapped=(0.15, 0.16),
                                 column_A=["b", "c"], column_D=(2, 3))
        # test invalid kwarg name
        with self.assertRaises(ValueError):
            filter_samples.filter_df(test_df, foo="foo")

    def test_filter_columns_numeric(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A": pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B": pd.Series(["A", "B", "C", "D"], dtype=object),
                                "column_C": pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D": pd.Series([1, 3, 5, 7], dtype=int)})
        # test filter on float series
        nptesting.assert_array_equal(filter_samples.filter_columns_numeric(test_df, column_C=(0.15, 0.35)).values,
                                     pd.DataFrame({"column_A": ["b", "c"], "column_B": ["B", "C"],
                                                   "column_C": [0.2, 0.3], "column_D": [3, 5]}).values)
        # test filter on int series
        nptesting.assert_array_equal(filter_samples.filter_columns_numeric(test_df, column_D=(2, 6)).values,
                                     pd.DataFrame({"column_A": ["b", "c"], "column_B": ["B", "C"],
                                                   "column_C": [0.2, 0.3], "column_D": [3, 5]}).values)
        # test filter on multiple series
        nptesting.assert_array_equal(filter_samples.filter_columns_numeric(test_df, column_D=(2, 4),
                                                                           column_C=(0.05, 0.35)).values,
                                     pd.DataFrame({"column_A": ["b"], "column_B": ["B"],
                                                   "column_C": [0.2], "column_D": [3]}).values)
        nptesting.assert_array_equal(filter_samples.filter_columns_numeric(test_df, **{"column_D": (3, 8),
                                                                           "column_C": (0.25, 0.35)}).values,
                                     pd.DataFrame({"column_A": ["c"], "column_B": ["C"],
                                                   "column_C": [0.3], "column_D": [5]}).values)
        # test empty output
        self.assertTrue(filter_samples.filter_columns_numeric(test_df, column_C=(0.23, 0.24)).empty)
        # test exceptions
        # invalid kwarg type
        with self.assertRaises(filter_samples.utils.InvalidDtype):
            filter_samples.filter_columns_numeric(test_df, column_A="foo")
        with self.assertRaises(filter_samples.utils.InvalidDtype):
            filter_samples.filter_columns_numeric(test_df, column_B="foo")
        # invlalid kwarg: is not in df.columns
        with self.assertRaises(KeyError):
            filter_samples.filter_columns_numeric(test_df, foo="foo")
        # invalid kwarg val: must be len(2)
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_numeric(test_df, column_D=(1, ))
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_numeric(test_df, column_D=(1, 2, 3))
        # invalid kwarg val: must be type list or tuple
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_numeric(test_df, column_D=1)
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_numeric(test_df, column_D="foo")
        # invalid kwarg val: must be len(2)
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_numeric(test_df, column_D=("foo",))
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_numeric(test_df, column_D=("foo", "bar", "baz"))
        # invalid kwarg val: elements must be numeric
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_numeric(test_df, column_D=("foo", "bar"))
        # invalid kwarg val: elements must be in order min followed by max
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_numeric(test_df, column_D=(2, 1))

    def test_filter_columns_categorical(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A": pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B": pd.Series(["A", "B", "C", "D"], dtype=object),
                                "column_C": pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D": pd.Series([1, 2, 3, 4], dtype=int)})
        # test filter on category series
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, column_A=["a"]).values,
                                     pd.DataFrame({"column_A": ["a"], "column_B": ["A"],
                                                   "column_C": [0.1], "column_D": [1]}).values)
        # test filter on object series
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, column_B=["B", "D"]).values,
                                     pd.DataFrame({"column_A": ["b", "d"], "column_B": ["B", "D"],
                                                   "column_C": [0.2, 0.4], "column_D": [2, 4]}).values)
        # test filter by excluding
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, not_column_B=["B", "D"]).values,
                                     pd.DataFrame({"column_A": ["a", "c"], "column_B": ["A", "C"],
                                                   "column_C": [0.1, 0.3], "column_D": [1, 3]}).values)
        # test filter by excluding and including - include followed by exclude
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, column_A=["a", "b"],
                                                                               not_column_B=["B", "D"]).values,
                                     pd.DataFrame({"column_A": ["a"], "column_B": ["A"],
                                                   "column_C": [0.1], "column_D": [1]}).values)
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, column_B=["A", "B", "C", "D"],
                                                                               not_column_A=["a", "b", "c", "d"]).values,
                                     pd.DataFrame({"coulmn_A": [], "column_B": [], "column_c": [], "column_d": []}).values)
        # test filter by excluding and including - exlcude followed by include
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, not_column_B=["B", "D"],
                                                                               column_A=["a", "b"]).values,
                                     pd.DataFrame({"column_A": ["a"], "column_B": ["A"],
                                                   "column_C": [0.1], "column_D": [1]}).values)
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, not_column_B=["A", "B", "C", "D"],
                                                                               column_A=["a", "b", "c", "d"]).values,
                                     pd.DataFrame({"coulmn_A": [], "column_B": [], "column_c": [], "column_d": []}).values)
        # test filter on multiple series
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, column_B=["B", "D"],
                                                                               column_A=["a", "b"]).values,
                                     pd.DataFrame({"column_A": ["b"], "column_B": ["B"],
                                                   "column_C": [0.2], "column_D": [2]}).values)
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, **{"column_B": ["B", "D"],
                                                                               "column_A": ["c", "d"]}).values,
                                     pd.DataFrame({"column_A": ["d"], "column_B": ["D"],
                                                   "column_C": [0.4], "column_D": [4]}).values)
        # test empty output
        self.assertTrue(filter_samples.filter_columns_categorical(test_df, column_A=["a", "b"],
                                                                  column_B=["C", "D"]).empty)
        # test warning
        # kwarg value is missing in column
        with self.assertWarns(Warning):
            filter_samples.filter_columns_categorical(test_df, column_A=["Z", "Y"], column_B=["x"])
        # test exceptions
        # invalid kwarg type
        with self.assertRaises(filter_samples.utils.InvalidDtype):
            filter_samples.filter_columns_categorical(test_df, column_C=[])
        # invalid kwarg type
        with self.assertRaises(filter_samples.utils.InvalidDtype):
            filter_samples.filter_columns_categorical(test_df, column_D=[])
        # invlalid kwarg: is not in df.columns
        with self.assertRaises(KeyError):
            filter_samples.filter_columns_categorical(test_df, foo="foo")
        # invalid kwarg type: must be list
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_categorical(test_df, column_A="a")
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_categorical(test_df, column_B=("A", "Pass"))
        # invalid kwarg type: must be list of strings
        with self.assertRaises(ValueError):
            filter_samples.filter_columns_categorical(test_df, column_A=[1, 2, 3])
