import unittest

import pandas as pd
import numpy.testing as nptesting

import build_snp_matrix

class TestBuildSnpMatrix(unittest.TestCase):
    def test_build_multi_fasta(self):
        pass

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

    # TODO: test individual filters, i.e. with only one kwarg
    # TODO: think of more test cases
    def test_filter_df(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d", "e"], dtype="object"),
                                "Outcome":pd.Series(["A", "Pass", "Pass", "Pass", "Pass"], dtype="category"), 
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.5], dtype=float),
                                "column_D":pd.Series([1, 2, 3, 4, 5], dtype=int)})
        # test multiple filters
        outcome = build_snp_matrix.filter_df(test_df, pcmap_threshold=(0.15, 0.45), 
                                                  column_A=["b", "c"], column_D=(1, 3)) 
        nptesting.assert_array_equal(outcome.values, pd.DataFrame({"column_A":["b"], "Outcome":["Pass"],
                                     "pcMapped":[0.2], "column_D":[2]}).values)
        # test empty output
        with self.assertRaises(Exception):
            build_snp_matrix.filter_df(test_df, pcmap_threshold=(0.15, 0.45), 
                                       column_A=["b", "c"], column_D=(2.5, 3))
        with self.assertRaises(ValueError):
            # invalid kwarg name
            build_snp_matrix.filter_df(test_df, foo="foo")

    def test_filter_column_numeric(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B":pd.Series(["A", "B", "C", "D"], dtype=object), 
                                "column_C":pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D":pd.Series([1, 2, 3, 4,], dtype=int)})
        # test filter on float series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_numeric(test_df, column_C=(0.1, 0.4)).values,
                                     pd.DataFrame({"column_A":["b", "c"], "column_B":["B", "C"], 
                                                   "column_C":[0.2, 0.3], "column_D":[2, 3]}).values)
        # test filter on int series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_numeric(test_df, column_D=(1, 4)).values,
                                      pd.DataFrame({"column_A":["b", "c"], "column_B":["B", "C"], 
                                                    "column_C":[0.2, 0.3], "column_D":[2, 3]}).values)
        # test filter on multiple series
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_numeric(test_df, column_D=(1, 4),
                                                                             column_C=(0.2, 0.4)).values,
                                     pd.DataFrame({"column_A":["c"], "column_B":["C"], 
                                                   "column_C":[0.3], "column_D":[3]}).values)
        nptesting.assert_array_equal(build_snp_matrix.filter_columns_numeric(test_df, **{"column_D": (1, 4),
                                                                             "column_C": (0.1, 0.3)}).values,
                                     pd.DataFrame({"column_A":["b"], "column_B":["B"], 
                                                   "column_C":[0.2], "column_D":[2]}).values)
        # test empty output
        self.assertTrue(build_snp_matrix.filter_columns_numeric(test_df, column_D=(2, 3)).empty)
        # test exceptions
        with self.assertRaises(build_snp_matrix.InvalidDtype):
            build_snp_matrix.filter_columns_numeric(test_df, column_A="foo")
            build_snp_matrix.filter_columns_numeric(test_df, column_B="foo")
        with self.assertRaises(KeyError):
            build_snp_matrix.filter_columns_numeric(test_df, foo="foo")
        with self.assertRaises(ValueError):
            # invalid kwarg val: must be len(2)
            build_snp_matrix.filter_columns_numeric(test_df, column_D=(1, ))
            build_snp_matrix.filter_columns_numeric(test_df, column_D=(1, 2, 3))

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
        self.assertTrue(build_snp_matrix.filter_columns_categorical(test_df, column_A=["E", "F"]).empty)
        # test exceptions
        with self.assertRaises(build_snp_matrix.InvalidDtype):
            build_snp_matrix.filter_columns_categorical(test_df, column_C=[])
            build_snp_matrix.filter_columns_categorical(test_df, column_D=[])
        with self.assertRaises(KeyError):
            build_snp_matrix.filter_columns_categorical(test_df, foo="foo")
        with self.assertRaises(ValueError):
            # invalid kwarg type: must be list
            build_snp_matrix.filter_columns_categorical(test_df, column_A="a")
            build_snp_matrix.filter_columns_categorical(test_df, column_B=("A", "Pass"))

    def test_build_multi_fasta(self):
        pass

    def test_append_multi_fasta(self):
        pass
    
    def test_snps(self):
        pass

if __name__ == "__main__":
    unittest.main()
