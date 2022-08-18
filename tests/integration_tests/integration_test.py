import unittest
import tempfile
import shutil
import os
from unittest import mock

import numpy.testing as nptesting
import pandas as pd

import btb_phylo

class IntegrationTestBtbPhylo(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    @mock.patch("utils.run")
    @mock.patch("update_summary.final_out_csv_to_df")
    def test_update_samples(self, mock_run, mock_s3_download_file):
        # mock utils.run() to return a string that's like what it would get from 'aws s3 ls s3://{bucket}/{prefix}/ --recursive | grep -e ".*FinalOut.*"'
            # this will essentially mock within the call to get_final_out_keys()
        mock_run.return_value = ""
        mock_s3_download_file.return_value = pd.read_csv("test/data/path/FinalOut.csv")
        test_metadata = {}
        summary_filepath = os.path.join(self.test_dir, "all_samples.csv")
        results_path = os.path.join(self.test_dir, "results")
        shutil.copy("test/data/path/all_samples.csv", summary_filepath)
        self.assertEquals(btb_phylo.update_samples(results_path, summary_filepath), test_metadata)
        nptesting.assert_equal(pd.read_csv(os.path.join(results_path, "metadata/all_samples.csv")), pd.read_csv("test/data/path/all_samples.csv"))
        nptesting.assert_equal(pd.read_csv(summary_filepath), pd.read_csv("test/data/path/all_samples_updated.csv"))
        
        
