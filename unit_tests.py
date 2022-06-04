from subprocess import check_call
import unittest

import pandas as pd

import build_snp_matrix

class TestBuildSnpMatrix(unittest.TestCase):
    def test_build_multi_fasta(self):
        pass

    def test_remove_duplicates(self):
        # define dataframe for input
        test_df = pd.DataFrame({"submission":pd.Series(["1", "2", "2", "2", "1", "3"], dtype="object"),
                                "pcMapped":pd.Series([0.1, 0.2, 0.3, 0.4, 0.1, 0.6], dtype=float)})
        # test expected input
        pd.testing.assert_frame_equal(build_snp_matrix.remove_duplicates(test_df, "pcMapped"),
                                      pd.DataFrame({"submission":["2", "3"], "pcMapped":[0.4, 0.6]}),
                                      check_dtype=False, check_categorical=False)

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
        pd.testing.assert_frame_equal(outcome, pd.DataFrame({"column_A":["b"], "Outcome":["Pass"],
                                      "pcMapped":[0.2], "column_D":[2]}), check_dtype=False, 
                                      check_categorical=False)
        # test empty output
        with self.assertRaises(Exception):
            build_snp_matrix.filter_df(test_df, pcmap_threshold=(0.15, 0.45), 
                                            column_A=["b", "c"], column_D=(2.5, 3))

    def test_filter_column_numeric(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B":pd.Series(["A", "B", "C", "D"], dtype=object), 
                                "column_C":pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D":pd.Series([1, 2, 3, 4,], dtype=int)})
        # test filter on float series
        pd.testing.assert_frame_equal(build_snp_matrix.filter_columns_numeric(test_df, "column_C", (0.1, 0.4)),
                                      pd.DataFrame({"column_A":["b", "c"], "column_B":["B", "C"], 
                                                    "column_C":[0.2, 0.3], "column_D":[2, 3]}),
                                                    check_dtype=False, check_categorical=False)
        # test filter on int series
        pd.testing.assert_frame_equal(build_snp_matrix.filter_columns_numeric(test_df, "column_D", (1, 4)),
                                      pd.DataFrame({"column_A":["b", "c"], "column_B":["B", "C"], 
                                                    "column_C":[0.2, 0.3], "column_D":[2, 3]}),
                                                    check_dtype=False, check_categorical=False)
        # test empty output
        self.assertTrue(build_snp_matrix.filter_columns_numeric(test_df, "column_D", (2, 3)).empty)
        # test exceptions
        with self.assertRaises(build_snp_matrix.InvalidDtype):
            build_snp_matrix.filter_columns_numeric(test_df, "column_A", "foo")
            build_snp_matrix.filter_columns_numeric(test_df, "column_B", "foo")
        with self.assertRaises(KeyError):
            build_snp_matrix.filter_columns_numeric(test_df, "foo", "foo")
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_numeric(test_df, "column_D", (1, ))
            build_snp_matrix.filter_columns_numeric(test_df, "column_D", (1, 2, 3))

    def test_filter_columns_categorical(self):
        # define dataframe for input
        test_df = pd.DataFrame({"column_A":pd.Series(["a", "b", "c", "d"], dtype="category"),
                                "column_B":pd.Series(["A", "B", "C", "D"], dtype=object), 
                                "column_C":pd.Series([0.1, 0.2, 0.3, 0.4], dtype=float),
                                "column_D":pd.Series([1, 2, 3, 4,], dtype=int)})
        # test filter on category series
        pd.testing.assert_frame_equal(build_snp_matrix.filter_columns_categorical(test_df, "column_A", ["a"]),
                                      pd.DataFrame({"column_A":["a"], "column_B":["A"], 
                                                    "column_C":[0.1], "column_D":[1]}),
                                                    check_dtype=False, check_categorical=False)
        # test filter on object series
        pd.testing.assert_frame_equal(build_snp_matrix.filter_columns_categorical(test_df, "column_B", 
                                      ["B", "D"]), pd.DataFrame({"column_A":["b", "d"], 
                                      "column_B":["B", "D"], "column_C":[0.2, 0.4], "column_D":[2, 4]}),
                                      check_dtype=False, check_categorical=False)
        # test empty output
        self.assertTrue(build_snp_matrix.filter_coluns_categorical(test_df, "column_A", ["E", "F"]).empty)
        # test exceptions
        with self.assertRaises(build_snp_matrix.InvalidDtype):
            build_snp_matrix.filter_columns_categorical(test_df, "column_C", [])
            build_snp_matrix.filter_columns_categorical(test_df, "column_D", [])
        with self.assertRaises(KeyError):
            build_snp_matrix.filter_columns_categorical(test_df, "foo", "foo")
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_columns_categorical(test_df, "column_A", 1)
            build_snp_matrix.filter_columns_categorical(test_df, "column_B", (1, 2, 3))

    def test_build_multi_fasta(self):
        pass

    def test_append_multi_fasta(self):
        pass
    
    def test_snps(self):
        pass

if __name__ == "__main__":
    unittest.main()