import unittest
from unittest import mock

import pandas as pd
import numpy.testing as nptesting

import build_snp_matrix


class TestBuildSnpMatrix(unittest.TestCase):
    def test_remove_duplicates(self):
        # define dataframe for input
        test_df = pd.DataFrame({"submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6], dtype=float)})
        # test expected input
        nptesting.assert_array_equal(build_snp_matrix.remove_duplicates(test_df, "pcMapped").values,
                                     pd.DataFrame({"submission":["2", "3"], "pcMapped":[0.4, 0.6]}).values)

    def test_get_indexes_to_remove(self):
        # define dataframe for input
        test_df = pd.DataFrame({"submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6], dtype=float)})
        # test expected input
        pd.testing.assert_index_equal(build_snp_matrix.get_indexes_to_remove(test_df, "pcMapped"),
                                      pd.Index([0, 1, 2, 4]), check_order=False)

    def test_filter_df(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d", "e"], dtype="object"),
                                "Outcome":pd.Series(["Fail", "Pass", "Pass", "Pass", "Pass"], dtype="category"), 
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.5], dtype=float),
                                "column_D":pd.Series([1, 3, 5, 7, 9], dtype=int)})
        # test individual filters
        outcome = build_snp_matrix.filter_df(test_df, pcmap_threshold=(0.1, 0.3))
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["b", "c"], "Outcome":["Pass", "Pass"],
                                     "pcMapped":[0.2, 0.3], "column_D":[3, 5]}).values)
        outcome = build_snp_matrix.filter_df(test_df, column_A=["a", "e"])
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["e"], "Outcome":["Pass"],
                                     "pcMapped":[0.5], "column_D":[9]}).values)
        outcome = build_snp_matrix.filter_df(test_df, column_D=(7, 10))
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["d", "e"], "Outcome":["Pass", "Pass"],
                                     "pcMapped":[0.4, 0.5], "column_D":[7, 9]}).values)
        # test multiple filters
        outcome = build_snp_matrix.filter_df(test_df, pcmap_threshold=(0.15, 0.45), 
                                                  column_A=["b", "c"], column_D=(2, 4)) 
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["b"], "Outcome":["Pass"],
                                     "pcMapped":[0.2], "column_D":[3]}).values)
        # test empty output
        with self.assertRaises(Exception):
            build_snp_matrix.filter_df(test_df, pcmap_threshold=(0.15, 0.16), 
                                       column_A=["b", "c"], column_D=(2, 3))
        # test invalid kwarg name
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_df(test_df, foo="foo")

    def test_filter_columns_numeric(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B":pd.Series(["A", "B", "C", "D"], dtype=object), 
                                "column_C":pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D":pd.Series([1, 3, 5, 7,], dtype=int)})
        # test filter on float series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_numeric(test_df, column_C=(0.15, 0.35)).values,
                                     pd.DataFrame({"column_A":["b", "c"], "column_B":["B", "C"], 
                                                   "column_C":[0.2, 0.3], "column_D":[3, 5]}).values)
        # test filter on int series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_numeric(test_df, column_D=(2, 6)).values,
                                      pd.DataFrame({"column_A":["b", "c"], "column_B":["B", "C"], 
                                                    "column_C":[0.2, 0.3], "column_D":[3, 5]}).values)
        # test filter on multiple series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_numeric(test_df, column_D=(2, 4),
                                                                             column_C=(0.05, 0.35)).values,
                                     pd.DataFrame({"column_A":["b"], "column_B":["B"], 
                                                   "column_C":[0.2], "column_D":[3]}).values)
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_numeric(test_df, **{"column_D": (3, 8),
                                                                             "column_C": (0.25, 0.35)}).values,
                                     pd.DataFrame({"column_A":["c"], "column_B":["C"], 
                                                   "column_C":[0.3], "column_D":[5]}).values)
        # test empty output
        self.assertTrue(build_snp_matrix.filter_columns_numeric(test_df, column_C=(0.23, 0.24)).empty)
        # test exceptions
        # invalid kwarg type
        with self.assertRaises(build_snp_matrix.InvalidDtype):
            build_snp_matrix.filter_columns_numeric(test_df, column_A="foo")
        with self.assertRaises(build_snp_matrix.InvalidDtype):
            build_snp_matrix.filter_columns_numeric(test_df, column_B="foo")
        # invlalid kwarg: is not in df.columns
        with self.assertRaises(KeyError):
            build_snp_matrix.filter_columns_numeric(test_df, foo="foo")
        # invalid kwarg val: must be len(2)
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_numeric(test_df, column_D=(1, ))
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_numeric(test_df, column_D=(1, 2, 3))
        # invalid kwarg val: must be type list or tuple
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_numeric(test_df, column_D=1)
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_numeric(test_df, column_D="foo")
        # invalid kwarg val: must be len(2)
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_numeric(test_df, column_D=("foo",))
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_numeric(test_df, column_D=("foo", "bar", "baz"))
        # invalid kwarg val: elements must be numeric 
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_numeric(test_df, column_D=("foo", "bar"))

    def test_filter_columns_categorical(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B":pd.Series(["A", "B", "C", "D"], dtype=object), 
                                "column_C":pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D":pd.Series([1, 2, 3, 4,], dtype=int)})
        # test filter on category series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_categorical(test_df, column_A=["a"]).values,
                                     pd.DataFrame({"column_A":["a"], "column_B":["A"], 
                                                   "column_C":[0.1], "column_D":[1]}).values)
        # test filter on object series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_categorical(test_df, column_B=["B", "D"]).values,
                                     pd.DataFrame({"column_A":["b", "d"], "column_B":["B", "D"], 
                                                   "column_C":[0.2, 0.4], "column_D":[2, 4]}).values)
        # test filter on multiple series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_categorical(test_df, column_B=["B", "D"],
                                                                                  column_A=["a", "b"]).values,
                                     pd.DataFrame({"column_A":["b"], "column_B":["B"], 
                                                   "column_C":[0.2], "column_D":[2]}).values)
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_categorical(test_df, **{"column_B": ["B", "D"],
                                                                                 "column_A": ["c", "d"]}).values,
                                     pd.DataFrame({"column_A":["d"], "column_B":["D"], 
                                                   "column_C":[0.4], "column_D":[4]}).values)
        # test empty output
        self.assertTrue(build_snp_matrix.filter_columns_categorical(test_df, column_A=["a", "b"], 
                                                                    column_B=["C", "D"]).empty)
        # test warning
        # kwarg value is missing in column
        with self.assertWarns(Warning):
            build_snp_matrix.filter_columns_categorical(test_df, column_A=["Z", "Y"], column_B=["x"])
        # test exceptions
        # invalid kwarg type
        with self.assertRaises(build_snp_matrix.InvalidDtype):
            build_snp_matrix.filter_columns_categorical(test_df, column_C=[])
        # invalid kwarg type
        with self.assertRaises(build_snp_matrix.InvalidDtype):
            build_snp_matrix.filter_columns_categorical(test_df, column_D=[])
        # invlalid kwarg: is not in df.columns
        with self.assertRaises(KeyError):
            build_snp_matrix.filter_columns_categorical(test_df, foo="foo")
        # invalid kwarg type: must be list
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_categorical(test_df, column_A="a")
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_categorical(test_df, column_B=("A", "Pass"))
        # invalid kwarg type: must be list of strings
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_categorical(test_df, column_A=[1, 2, 3])

    def test_build_multi_fasta(self):
        # test dataframe for input - 4 rows imitating 4 samples
        test_df = pd.DataFrame({"sample_name": ["A", "B", "C", "D"], 
                                "results_prefix": ["a", "b", "c", "d"], 
                                "results_bucket": ["1", "2", "3", "4"]})
        mock_open =  mock.mock_open()
        # mock open.read() to return 4 mock consensus sequences
        mock_open().read.side_effect=["AAA\nAAA", "TTT\nTTT", "CCC\nCCC", "GGG\nGGG"]
        # mock utils.s3_download_file
        build_snp_matrix.utils.s3_download_file = mock.Mock()
        # run build_multi_fasta() with test_df and a patched open
        with mock.patch("builtins.open", mock_open):
            build_snp_matrix.build_multi_fasta("foo", test_df)
        # calls to open to test against: 1 call for the output 
        # multifasta ("foo") and 4 calls for each consensus sequence
        open_calls = [mock.call("foo", "wb"), 
                      mock.call(mock.ANY, "rb"), 
                      mock.call(mock.ANY, "rb"),  
                      mock.call(mock.ANY, "rb"), 
                      mock.call(mock.ANY, "rb")]
        # assert that open was called with open_calls
        mock_open.assert_has_calls(open_calls, any_order=True)
        # assert that open.write() was called with mock consensus sequences
        write_calls = [mock.call("AAA\nAAA"), 
                       mock.call("TTT\nTTT"), 
                       mock.call("CCC\nCCC"), 
                       mock.call("GGG\nGGG")]
        mock_open().write.assert_has_calls(write_calls)
    
    def snp_sites(self):
        pass

    def snp_dists(self):
        pass

if __name__ == "__main__":
    unittest.main()
