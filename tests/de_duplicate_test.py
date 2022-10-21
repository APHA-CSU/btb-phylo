import unittest
from unittest import mock

import pandas as pd
import numpy.testing as nptesting

import btbphylo.de_duplicate as de_duplicate


class TestDeDuplicate(unittest.TestCase):
    def test_remove_duplicates(self):
        # test normal operation
        # test input
        test_df = pd.DataFrame({"Submission":pd.Series(["A", "B", "C", "D", "E", "F"], dtype="object"),
                                "foo":pd.Series([1, 2, 3, 4, 5, 6], dtype=float),
                                "bar":pd.Series([1, 2, 3, 4, 5, 6], dtype=float)})
        # mock get_indexes_to_remove
        with mock.patch("btbphylo.de_duplicate.get_indexes_to_remove") as mock_get_indexes_to_remove:
            # with side effects
            mock_get_indexes_to_remove.side_effect = [pd.Index([0, 1]),
                                                      pd.Index([2])]
            # assert output
            nptesting.assert_array_equal(de_duplicate.remove_duplicates(test_df, foo="max", bar="min").values,
                                         pd.DataFrame({"Submission":["D", "E", "F"], 
                                                       "pcMapped":[4, 5, 6], "Ncount":[4, 5, 6]}).values)
            actual_get_index_to_remove_calls = mock_get_indexes_to_remove.call_args_list
            # assert calls to get_indexes_to_remove
            nptesting.assert_array_equal(actual_get_index_to_remove_calls[0][0][0], test_df.loc[pd.Index([0, 1, 2, 3, 4, 5])])
            nptesting.assert_array_equal(actual_get_index_to_remove_calls[1][0][0], test_df.loc[pd.Index([2, 3, 4, 5])])
        # test defaulting to first sample
        test_df = pd.DataFrame({"Submission":pd.Series(["A", "A"], dtype="object"),
                                "foo":pd.Series([1, 2], dtype=float)})
        with mock.patch("btbphylo.de_duplicate.get_indexes_to_remove") as mock_get_indexes_to_remove:
            # side effect is to return no indexes, i.e. don't remove any entries
            mock_get_indexes_to_remove.side_effect = [pd.Index([])]
            nptesting.assert_array_equal(de_duplicate.remove_duplicates(test_df, foo="max").values,
                                         pd.DataFrame({"Submission":["A"], "foo":[1]}).values)
        # test exceptions
        with self.assertRaises(ValueError):
            de_duplicate.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                            "foo":pd.Series([1], dtype=float)}), bar="max")
        with self.assertRaises(ValueError):
            de_duplicate.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                           "foo":pd.Series([1], dtype=float)}), foo="bar")
        with self.assertRaises(ValueError):
            de_duplicate.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["bar"], dtype="object"),
                                           "foo":pd.Series([1], dtype=object)}), foo="baz")
        with self.assertRaises(TypeError):
            de_duplicate.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                           "foo":pd.Series([1], dtype=float)}))

    def test_get_indexes_to_remove(self):
        # test max
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "1", "2", "2", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.5], dtype=float)})
        pd.testing.assert_index_equal(de_duplicate.get_indexes_to_remove(test_df, "pcMapped", "max"),
                                      pd.Index([0, 2]), check_order=False)
        # test min
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "1", "2", "2", "3"], dtype="object"),
                                "Ncount":pd.Series([1, 2, 3, 4, 5], dtype=float)})
        pd.testing.assert_index_equal(de_duplicate.get_indexes_to_remove(test_df, "Ncount", "min"),
                                      pd.Index([1, 3]), check_order=False)
        # test removing multiple duplicates of the same submission 
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "1", "2", "2", "2"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.5], dtype=float)})
        pd.testing.assert_index_equal(de_duplicate.get_indexes_to_remove(test_df, "pcMapped", "max"),
                                      pd.Index([0, 2, 3]), check_order=False)
        # test categorical
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "1", "2", "2", "2"], dtype="object"),
                                "Outcome":pd.Series(["Pass", "Fail", "Fail", "Pass", "Fail"], dtype="object")})
        pd.testing.assert_index_equal(de_duplicate.get_indexes_to_remove(test_df, "Outcome", "Pass"),
                                      pd.Index([1, 2, 4]), check_order=False)
        # test keeping entries if not duplicated
        test_df = pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                "Outcome":pd.Series(["Fail"], dtype="object")})
        pd.testing.assert_index_equal(de_duplicate.get_indexes_to_remove(test_df, "Outcome", "Pass"),
                                      pd.Index([]), check_order=False)