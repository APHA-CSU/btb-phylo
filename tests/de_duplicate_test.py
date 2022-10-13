
import unittest

import pandas as pd
import numpy.testing as nptesting

from btbphylo import de_duplicate


class TestDeDuplicate(unittest.TestCase):
    def test_remove_duplicates(self):
        # test 2 kwargs
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6], dtype=float)})
        nptesting.assert_array_equal(de_duplicate.remove_duplicates(test_df, pcMapped="max", Ncount="min").values,
                                     pd.DataFrame({"Submission":["1", "2", "3"], 
                                                   "pcMapped":[0.1, 0.4, 0.6], "Ncount":[1, 4, 6]}).values)
        # test 3 kwargs
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3", "1"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6, 0.1], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6, 1], dtype=float),
                                "foo":pd.Series([10, 20, 30, 40, 50, 60, 70], dtype=float)})
        nptesting.assert_array_equal(de_duplicate.remove_duplicates(test_df, pcMapped="max", Ncount="min", foo="max").values,
                                     pd.DataFrame({"Submission":["2", "3", "1"], 
                                                   "pcMapped":[0.4, 0.6, 0.1], "Ncount":[4, 6, 1], "foo":[40, 60, 70]}).values)
        # test defaulting to first sample
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3", "1"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6, 0.1], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6, 1], dtype=float),
                                "foo":pd.Series([10, 20, 30, 40, 50, 60, 10], dtype=float)})
        nptesting.assert_array_equal(de_duplicate.remove_duplicates(test_df, pcMapped="max", Ncount="min", foo="max").values,
                                     pd.DataFrame({"Submission":["1", "2", "3"], 
                                                   "pcMapped":[0.1, 0.4, 0.6], "Ncount":[1, 4, 6], "foo":[10, 40, 60]}).values)
        # test exceptions
        with self.assertRaises(de_duplicate.utils.InvalidDtype):
            de_duplicate.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                        "foo":pd.Series(["a"], dtype="object")}), foo="max")
        with self.assertRaises(ValueError):
            de_duplicate.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                        "foo":pd.Series([1], dtype=float)}), bar="max")
        with self.assertRaises(ValueError):
            de_duplicate.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                        "foo":pd.Series([1], dtype=float)}), foo="bar")
        with self.assertRaises(TypeError):
            de_duplicate.remove_duplicates(pd.DataFrame({"Submission":pd.Series(["1"], dtype="object"),
                                        "foo":pd.Series([1], dtype=float)}))

    def test_get_indexes_to_remove(self):
        # test max
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.2, 0.6], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6], dtype=float)})
        pd.testing.assert_index_equal(de_duplicate.get_indexes_to_remove(test_df, "pcMapped", "max"),
                                      pd.Index([0, 1, 2]), check_order=False)
        # test min
        test_df = pd.DataFrame({"Submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6], dtype=float),
                                "Ncount":pd.Series([1, 2, 3, 4, 5, 6], dtype=float)})
        pd.testing.assert_index_equal(de_duplicate.get_indexes_to_remove(test_df, "Ncount", "min"),
                                      pd.Index([2, 3, 4]), check_order=False)