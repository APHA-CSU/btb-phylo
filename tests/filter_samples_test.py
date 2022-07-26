import unittest

import pandas as pd
import numpy.testing as nptesting

from btbphylo import filter_samples


class TestFilterSamples(unittest.TestCase):
    def test_remove_duplicates(self):
        # test 2 kwargs
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6], dtype=float)})
        nptesting.assert_array_equal(filter_samples.remove_duplicates(test_df, pcMapped="max", Ncount="min").values,
                                     pd.DataFrame({"Submission":["1", "2", "3"], 
                                                   "pcMapped":[0.1, 0.4, 0.6], "Ncount":[1, 4, 6]}).values)
        # test 3 kwargs
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3", "1"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6, 0.1], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6, 1], dtype=float),
                                "foo":pd.Series([10, 20, 30, 40, 50, 60, 70], dtype=float)})
        nptesting.assert_array_equal(filter_samples.remove_duplicates(test_df, pcMapped="max", Ncount="min", foo="max").values,
                                     pd.DataFrame({"Submission":["2", "3", "1"], 
                                                   "pcMapped":[0.4, 0.6, 0.1], "Ncount":[4, 6, 1], "foo":[40, 60, 70]}).values)
        # test defaulting to first sample
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3", "1"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6, 0.1], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6, 1], dtype=float),
                                "foo":pd.Series([10, 20, 30, 40, 50, 60, 10], dtype=float)})
        nptesting.assert_array_equal(filter_samples.remove_duplicates(test_df, pcMapped="max", Ncount="min", foo="max").values,
                                     pd.DataFrame({"Submission":["1", "2", "3"], 
                                                   "pcMapped":[0.1, 0.4, 0.6], "Ncount":[1, 4, 6], "foo":[10, 40, 60]}).values)
        # test exceptions
        with self.assertRaises(filter_samples.InvalidDtype):
            filter_samples.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                        "foo":pd.Series(["a"], dtype="object")}), foo="max")
        with self.assertRaises(ValueError):
            filter_samples.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                        "foo":pd.Series([1], dtype=float)}), bar="max")
        with self.assertRaises(ValueError):
            filter_samples.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                        "foo":pd.Series([1], dtype=float)}), foo="bar")
        with self.assertRaises(TypeError):
            filter_samples.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                        "foo":pd.Series([1], dtype=float)}))

    def test_get_indexes_to_remove(self):
        # test max
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.2, 0.6], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6], dtype=float)})
        pd.testing.assert_index_equal(filter_samples.get_indexes_to_remove(test_df, "pcMapped", "max"),
                                      pd.Index([0, 1, 2]), check_order=False)
        # test min
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6], dtype=float)})
        pd.testing.assert_index_equal(filter_samples.get_indexes_to_remove(test_df, "Ncount", "min"),
                                      pd.Index([2, 3, 4]), check_order=False)

    def test_filter_df(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d", "e"], dtype="object"),
                                "Outcome":pd.Series(["Fail", "Pass", "Pass", "Pass", "Pass"], dtype="category"), 
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.5], dtype=float),
                                "column_D":pd.Series([1, 3, 5, 7, 9], dtype=int)})
        # test individual filters
        outcome = filter_samples.filter_df(test_df, pcMapped=(0.1, 0.3))
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["b", "c"], "Outcome":["Pass", "Pass"],
                                     "pcMapped":[0.2, 0.3], "column_D":[3, 5]}).values)
        outcome = filter_samples.filter_df(test_df, column_A=["a", "b", "e"])
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["b", "e"], "Outcome":["Pass", "Pass"],
                                     "pcMapped":[0.2, 0.5], "column_D":[3, 9]}).values)
        outcome = filter_samples.filter_df(test_df, column_D=(7, 10))
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["d", "e"], "Outcome":["Pass", "Pass"],
                                     "pcMapped":[0.4, 0.5], "column_D":[7, 9]}).values)
        # test multiple filters
        outcome = filter_samples.filter_df(test_df, pcMapped=(0.15, 0.45), 
                                           column_A=["a", "b", "d", "e"], column_D=(2, 8)) 
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["b", "d"], "Outcome":["Pass", "Pass"],
                                     "pcMapped":[0.2, 0.4], "column_D":[3, 7]}).values)
        outcome = filter_samples.filter_df(test_df, pcMapped=(0.05, 0.45), 
                                           column_A=["a", "b", "d", "e"], column_D=(0.5, 8), Outcome=["Pass", "Fail"]) 
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["a", "b", "d"], "Outcome":["Fail", "Pass", "Pass"],
                                     "pcMapped":[0.1, 0.2, 0.4], "column_D":[1, 3, 7]}).values)
        # test no filters
        outcome = filter_samples.filter_df(test_df)
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":pd.Series(["b", "c", "d", "e"], dtype="object"),
                                                                  "Outcome":pd.Series(["Pass", "Pass", "Pass", "Pass"], dtype="category"), 
                                                                  "pcMapped":pd.Series([0.2, 0.3, 0.4, 0.5], dtype=float),
                                                                  "column_D":pd.Series([3, 5, 7, 9], dtype=int)}).values)
        # test empty output < 1 samples
        # empty
        with self.assertRaises(Exception):
            filter_samples.filter_df(test_df, pcMapped=(0.15, 0.16), 
                                       column_A=["b", "c"], column_D=(2, 3))
        # 1 sample
        with self.assertRaises(Exception):
            filter_samples.filter_df(test_df, pcMapped=(0.15, 0.25), 
                                       column_A=["b", "c"], column_D=(2, 4))
        # test invalid kwarg name
        with self.assertRaises(ValueError):
            filter_samples.filter_df(test_df, foo="foo")

    def test_filter_columns_numeric(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B":pd.Series(["A", "B", "C", "D"], dtype=object), 
                                "column_C":pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D":pd.Series([1, 3, 5, 7,], dtype=int)})
        # test filter on float series
        nptesting.assert_array_equal(filter_samples.filter_columns_numeric(test_df, column_C=(0.15, 0.35)).values,
                                     pd.DataFrame({"column_A":["b", "c"], "column_B":["B", "C"], 
                                                   "column_C":[0.2, 0.3], "column_D":[3, 5]}).values)
        # test filter on int series
        nptesting.assert_array_equal(filter_samples.filter_columns_numeric(test_df, column_D=(2, 6)).values,
                                      pd.DataFrame({"column_A":["b", "c"], "column_B":["B", "C"], 
                                                    "column_C":[0.2, 0.3], "column_D":[3, 5]}).values)
        # test filter on multiple series
        nptesting.assert_array_equal(filter_samples.filter_columns_numeric(test_df, column_D=(2, 4),
                                                                             column_C=(0.05, 0.35)).values,
                                     pd.DataFrame({"column_A":["b"], "column_B":["B"], 
                                                   "column_C":[0.2], "column_D":[3]}).values)
        nptesting.assert_array_equal(filter_samples.filter_columns_numeric(test_df, **{"column_D": (3, 8),
                                                                             "column_C": (0.25, 0.35)}).values,
                                     pd.DataFrame({"column_A":["c"], "column_B":["C"], 
                                                   "column_C":[0.3], "column_D":[5]}).values)
        # test empty output
        self.assertTrue(filter_samples.filter_columns_numeric(test_df, column_C=(0.23, 0.24)).empty)
        # test exceptions
        # invalid kwarg type
        with self.assertRaises(filter_samples.InvalidDtype):
            filter_samples.filter_columns_numeric(test_df, column_A="foo")
        with self.assertRaises(filter_samples.InvalidDtype):
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
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B":pd.Series(["A", "B", "C", "D"], dtype=object), 
                                "column_C":pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D":pd.Series([1, 2, 3, 4,], dtype=int)})
        # test filter on category series
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, column_A=["a"]).values,
                                     pd.DataFrame({"column_A":["a"], "column_B":["A"], 
                                                   "column_C":[0.1], "column_D":[1]}).values)
        # test filter on object series
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, column_B=["B", "D"]).values,
                                     pd.DataFrame({"column_A":["b", "d"], "column_B":["B", "D"], 
                                                   "column_C":[0.2, 0.4], "column_D":[2, 4]}).values)
        # test filter on multiple series
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, column_B=["B", "D"],
                                                                                  column_A=["a", "b"]).values,
                                     pd.DataFrame({"column_A":["b"], "column_B":["B"], 
                                                   "column_C":[0.2], "column_D":[2]}).values)
        nptesting.assert_array_equal(filter_samples.filter_columns_categorical(test_df, **{"column_B": ["B", "D"],
                                                                                 "column_A": ["c", "d"]}).values,
                                     pd.DataFrame({"column_A":["d"], "column_B":["D"], 
                                                   "column_C":[0.4], "column_D":[4]}).values)
        # test empty output
        self.assertTrue(filter_samples.filter_columns_categorical(test_df, column_A=["a", "b"], 
                                                                    column_B=["C", "D"]).empty)
        # test warning
        # kwarg value is missing in column
        with self.assertWarns(Warning):
            filter_samples.filter_columns_categorical(test_df, column_A=["Z", "Y"], column_B=["x"])
        # test exceptions
        # invalid kwarg type
        with self.assertRaises(filter_samples.InvalidDtype):
            filter_samples.filter_columns_categorical(test_df, column_C=[])
        # invalid kwarg type
        with self.assertRaises(filter_samples.InvalidDtype):
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