import unittest
from unittest import mock
import argparse

import pandas as pd
import numpy.testing as nptesting

import btbphylo.phylogeny as phylogeny 
import btbphylo.filter_samples as filter_samples
import btbphylo.update_summary as update_summary


class TestPhylogeny(unittest.TestCase):
    @mock.patch("btbphylo.phylogeny.utils.s3_download_file_cli")
    @mock.patch("btbphylo.phylogeny.extract_s3_bucket")
    @mock.patch("btbphylo.phylogeny.extract_s3_key")
    def test_build_multi_fasta(self, _, mock_extract_s3_bucket, mock_extract_s3_key):
        mock_extract_s3_bucket.return_value = "foo_bucket"
        mock_extract_s3_key.return_value = "foo_key"
        # test dataframe for input - 4 rows imitating 4 samples
        test_df = pd.DataFrame({"Sample": ["A", "B", "C", "D"], 
                                "ResultLoc": ["1", "2", "3", "4"]})
        mock_open =  mock.mock_open()
        # mock open.read() to return 4 mock consensus sequences
        mock_open().read.side_effect=["AAA\nAAA", "TTT\nTTT", "CCC\nCCC", "GGG\nGGG"]
        # run build_multi_fasta() with test_df and a patched open
        with mock.patch("builtins.open", mock_open):
            phylogeny.build_multi_fasta("foo", test_df, 'bar')
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
    
    def test_extract_s3_bucket(self):
        # test good input
        test_input = ["s3://s3-csu-003/abc/123/",
                      "s3://s3-csu-123//5/1",
                      "s3://s3-csu-001///"]
        test_output = ["s3-csu-003",
                       "s3-csu-123", 
                       "s3-csu-001"] 
        fail = False 
        i = 0
        for input, output in zip(test_input, test_output):
            try:
                self.assertEqual(phylogeny.extract_s3_bucket(input), output)
            except AssertionError as e:
                i += 1
                fail = True
                print(f"Test failure {i}: ", e)
        if fail: 
            print(f"{i} test failures")

    def test_extract_s3_key(self):
        # test good input
        test_input = [("s3://s3-csu-003/abc/123/", "foo"),
                      ("s3://s3-csu-123//5/1", "bar"),
                      ("s3://s3-csu-001///", "baz")]
        test_output = ["abc/123/consensus/foo_consensus.fas",
                       "5/1/consensus/bar_consensus.fas", 
                       "consensus/baz_consensus.fas"] 
        fail = False 
        i = 0
        for input, output in zip(test_input, test_output):
            try:
                self.assertEqual(phylogeny.extract_s3_key(input[0], input[1]), output)
            except AssertionError as e:
                i += 1
                fail = True
                print(f"Test failure {i}: ", e)
        if fail: 
            print(f"{i} test failures")

    def test_match_s3_uri(self):
        # test exceptions
        with self.assertRaises(phylogeny.BadS3UriError):
            phylogeny.match_s3_uri('s3://s3-csu-003')
        with self.assertRaises(phylogeny.BadS3UriError):
            phylogeny.match_s3_uri('s3://s3-csu-003abc')
        with self.assertRaises(phylogeny.BadS3UriError):
            phylogeny.match_s3_uri('s3://s3-csu-03/abc')
        with self.assertRaises(phylogeny.BadS3UriError):
            phylogeny.match_s3_uri('s3:/s3-csu-003/abc')
        with self.assertRaises(phylogeny.BadS3UriError):
            phylogeny.match_s3_uri('s4://s3-csu-003/abc')
        with self.assertRaises(phylogeny.BadS3UriError):
            phylogeny.match_s3_uri('s3://s5-abc-003/abc')
        with self.assertRaises(phylogeny.BadS3UriError):
            phylogeny.match_s3_uri('s3://s3-csu-abc/abc')
        with self.assertRaises(phylogeny.BadS3UriError):
            phylogeny.match_s3_uri('s3://s3-csu-1234/abc')

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
        #nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["b", "e"], "Outcome":["Pass", "Pass"],
        #                             "pcMapped":[0.2, 0.5], "column_D":[3, 9]}).values)
        outcome = filter_samples.filter_df(test_df, column_D=(7, 10))
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["d", "e"], "Outcome":["Pass", "Pass"],
                                     "pcMapped":[0.4, 0.5], "column_D":[7, 9]}).values)
        # test multiple filters
        outcome = filter_samples.filter_df(test_df, pcMapped=(0.15, 0.45), 
                                                  column_A=["a", "b", "d", "e"], column_D=(2, 8)) 
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["b", "d"], "Outcome":["Pass", "Pass"],
                                     "pcMapped":[0.2, 0.4], "column_D":[3, 7]}).values)
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

class TestUpdateSummary(unittest.TestCase):
    def test_extract_submission_no(self):
        # Test cases
        test_input = ["AFxx-12-34567-89",
                      "ATxx-12-34567-89",
                      "AFx-12-34567-89",
                      "Ax-12-34567-89",
                      "AF-12-34567-89",
                      "AFx12-34567-89",
                      "HI-12-34567-89",
                      "12-34567-89-1L",
                      "12-34567-89-L1",
                      "A-12-34567-89",
                      "12-34567-89-1",
                      "12-34567-89-L",
                      "12-34567-89",
                      "AFxx-12-3456-89",
                      "ATxx-12-3456-89",
                      "AFx-12-3456-89",
                      "Ax-12-3456-89",
                      "AF-12-3456-89",
                      "AFx12-3456-89",
                      "HI-12-3456-89",
                      "12-3456-89-1L",
                      "12-3456-89-L1",
                      "A-12-3456-89",
                      "12-3456-89-1",
                      "12-3456-89-L",
                      "12-3456-89",
                      "12345678",
                      "ABCDEFGH",
                      ""]
        test_output = ["12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-34567-89",
                       "12-3456-89",
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12-3456-89", 
                       "12345678", 
                       "ABCDEFGH", 
                       ""] 
        fail = False 
        i = 0
        for input, output in zip(test_input, test_output):
            try:
                self.assertEqual(update_summary.extract_submission_no(input), output)
            except AssertionError as e:
                i += 1
                fail = True
                print(f"Test failure {i}: ", e)
        if fail: 
            print(f"{i} test failures")
            raise AssertionError

    @mock.patch("btbphylo.update_summary.finalout_csv_to_df")
    def test_append_df_summary(self, mock_finalout_csv_to_df):
        mock_finalout_csv_to_df.return_value = pd.DataFrame()
        # simulate 7 new keys
        test_new_keys = [0, 1, 2, 3, 4, 5, 6]
        # start with empty df_summary
        test_df_summary = pd.DataFrame({"foo":[], "bar":[], "baz":[]})
        with mock.patch("btbphylo.update_summary.add_submission_col") as mock_add_submission_col:
            # mock sequential return values of calls to update_summary.add_submission_col, 
            # this effectively mocks the return value of finalout_csv_to_df(new_keys[itteration]).pipe(add_submission_col)
            mock_add_submission_col.side_effect = [pd.DataFrame({"foo":["a"], "bar":["a"], "baz":["a"]}),
                                                   pd.DataFrame({"foo":["b"], "bar":["b"], "baz":["b"]}),
                                                   pd.DataFrame({"foo":["c"], "bar":["c"], "baz":["c"]}),
                                                   pd.DataFrame({"foo":["d"], "bar":["d"], "baz":["d"]}),
                                                   pd.DataFrame({"foo":["e"], "bar":["e"], "baz":["e"]}),
                                                   pd.DataFrame({"foo":["f"], "bar":["f"], "baz":["f"]}),
                                                   pd.DataFrame({"foo":["g"], "bar":["g"], "baz":["g"]})]
            test_output = update_summary.append_df_summary(test_df_summary, test_new_keys)
            # assert correct output
            nptesting.assert_array_equal(test_output,
                                         pd.DataFrame({"foo":["a", "b", "c", "d", "e", "f", "g"], 
                                                       "bar":["a", "b", "c", "d", "e", "f", "g"], 
                                                       "baz":["a", "b", "c", "d", "e", "f", "g"]}).values)
            finalout_csv_to_df_calls = [mock.call(0), mock.call(1), mock.call(2), mock.call(3),
                                        mock.call(4), mock.call(5), mock.call(6)]
            # assert recursion was called with correct order of arguments
            mock_finalout_csv_to_df.assert_has_calls(finalout_csv_to_df_calls)

    def test_get_finalout_s3_keys(self):
        # mock AWS s3 CLI command for getting s3 metadata
        with mock.patch("btbphylo.update_summary.utils.run") as mock_run:
            mock_run.return_value = "2022-06-28 06:38:51      45876 v3-2/Results_10032_27Jun22/10032_FinalOut_28Jun22.csv\n \
                                     2022-06-28 06:38:51 45876 v3-2/Results_10032_27Jun22/bar\n \
                                     a b c foo" 
            test_output = ["v3-2/Results_10032_27Jun22/10032_FinalOut_28Jun22.csv",
                           "v3-2/Results_10032_27Jun22/bar",
                           "foo"]
            self.assertEqual(update_summary.get_finalout_s3_keys(), test_output)

    def test_extract_s3_key(self):
        #test cases
        test_input = ["2022-06-28 06:38:51      45876 v3-2/Results_10032_27Jun22/10032_FinalOut_28Jun22.csv",
                      "2022-06-28 06:38:51 45876 v3-2/Results_10032_27Jun22/bar",
                      "a b c foo"]
        test_output = ["v3-2/Results_10032_27Jun22/10032_FinalOut_28Jun22.csv",
                       "v3-2/Results_10032_27Jun22/bar",
                       "foo"]
        fail = False
        for input, output in zip(test_input, test_output):
            try:
                self.assertEqual(update_summary.extract_s3_key(input), output)
            except AssertionError as e:
                fail = True
                print(e)
        if fail:
            raise AssertionError

                                                    
def test_suit(test_objs):                           
    suit = unittest.TestSuite(test_objs)            
    return suit                                     
                                                    
if __name__ == "__main__":                          
    phylogeny_test = [TestPhylogeny('test_build_multi_fasta'),
                      TestPhylogeny('test_extract_s3_bucket'),
                      TestPhylogeny('test_match_s3_uri')]
    filter_samples_test = [TestFilterSamples('test_remove_duplicates'),
                           TestFilterSamples('test_get_indexes_to_remove'),
                           TestFilterSamples('test_filter_df'),
                           TestFilterSamples('test_filter_columns_numeric'),
                           TestFilterSamples('test_filter_columns_categorical')]
    update_summary_test = [TestUpdateSummary('test_extract_submission_no'),
                           TestUpdateSummary('test_append_df_summary'),
                           TestUpdateSummary('test_get_finalout_s3_keys'),
                           TestUpdateSummary('test_extract_s3_key')]
    runner = unittest.TextTestRunner()
    parser = argparse.ArgumentParser(description='Test code')
    module_arg = parser.add_argument('--module', '-m', nargs=1, 
                                     help="module to test: 'phylogeny', 'update_summary' or 'filter_samples'",
                                     default=None)
    args = parser.parse_args()
    if args.module:
        if args.module[0] == 'phylogeny':
            runner.run(test_suit(phylogeny_test)) 
        elif args.module[0] == 'filter_samples':
            runner.run(test_suit(filter_samples_test)) 
        elif args.module[0] == 'update_summary':
            runner.run(test_suit(update_summary_test)) 
        else:
            raise argparse.ArgumentError(module_arg, 
                                         "Invalid argument. Please use 'phylogeny', 'update_summary' or 'filter_samples'")
    else:
        unittest.main(buffer=True)