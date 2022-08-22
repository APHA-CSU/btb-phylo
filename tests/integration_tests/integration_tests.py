import unittest
import tempfile
import shutil
import os
import sys
import json
from unittest import mock

import pandas as pd
import pandas.testing as pdtesting

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from btb_phylo import *

DEFAULT_TESTDATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class IntegrationTestBtbPhylo(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    @mock.patch("btb_phylo.update_summary.finalout_csv_to_df")
    @mock.patch("btb_phylo.update_summary.utils.run")
    def test_update_samples(self, mock_run, mock_finalout_csv_to_df, 
                            testdata_path=os.path.join(DEFAULT_TESTDATA_PATH, "update_samples")):
        with open(os.path.join(testdata_path, "finalout_s3_data.txt")) as finalout_s3_data:
            mock_run.return_value = finalout_s3_data.read()
        mock_finalout_csv_to_df.side_effect = [pd.read_csv(os.path.join(testdata_path, "FinalOut_1.csv"), comment="#"),
                                               pd.read_csv(os.path.join(testdata_path, "FinalOut_2.csv"), comment="#"),
                                               pd.read_csv(os.path.join(testdata_path, "FinalOut_3.csv"), comment="#")]
        test_metadata = {"total_number_of_wgs_samples": 12}
        summary_filepath = os.path.join(self.test_dir, "all_samples.csv")
        results_path = os.path.join(self.test_dir, "results")
        shutil.copy(os.path.join(testdata_path, "all_samples_test.csv"), summary_filepath)
        self.assertEqual(update_samples(results_path, summary_filepath)[0], test_metadata)
        pdtesting.assert_frame_equal(utils.summary_csv_to_df(summary_filepath), 
                                     utils.summary_csv_to_df(os.path.join(testdata_path, "all_samples_updated.csv")))

    def test_sample_filter(self, testdata_path=os.path.join(DEFAULT_TESTDATA_PATH, "filter_samples")):
        summary_filepath = os.path.join(testdata_path, "all_samples_test.csv")
        test_filtered_summary_filepath = os.path.join(testdata_path, "filtered_all_samples_test.csv")
        results_path = os.path.join(self.test_dir, "results")
        metadata_dir = os.path.join(self.test_dir, "metadata")
        os.makedirs(os.path.join(results_path, "metadata"))
        with open(os.path.join(metadata_dir, "metadata.json"), "w") as f:
            json.dump({"total_number_of_wgs_samples": 12}, f, indent=2)
        (metadata, df_filtered) = sample_filter(results_path, summary_filepath, 
                                                config=os.path.join(testdata_path, "example_config.json"))
        test_df_filtered = utils.summary_csv_to_df(test_filtered_summary_filepath)
        pdtesting.assert_frame_equal(df_filtered, test_df_filtered)
    
if __name__ == "__main__":
    unittest.main()
