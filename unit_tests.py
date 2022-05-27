import unittest

import pandas as pd

import build_snp_matrix

TEST_CSV_PATH = "test_files/test.csv"

class TestBuildSnpMatrix(unittest.TestCase):
    def test_build_multi_fasta(self):
        pass

    def test_filter_samples(self):
        test_df = pd.read_csv(TEST_CSV_PATH)
        with self.assertRaises(ValueError):
            build_snp_matrix.filter_samples(test_df, [1,])
            build_snp_matrix.filter_samples(test_df, (1,))
            build_snp_matrix.filter_samples(test_df, ("a","b"))
            build_snp_matrix.filter_samples(test_df, [1, 2, 3])
            build_snp_matrix.filter_samples(test_df, ["a", 2])

    def test_append_multi_fasta(self):
        pass
    
    def test_snps(self):
        pass

if __name__ == "__main__":
    unittest.main()