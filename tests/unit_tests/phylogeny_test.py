import unittest
from unittest import mock

import pandas as pd
import numpy.testing as nptesting

from btbphylo import phylogeny


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

    def test_process_sample_name(self):
        # test cases
        test_input = ["AFxx-12-34567-89",
                      "AFxx-12-34567-89_consensus",
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
                      "ABcDeFgh",
                      "1BcD2Fgh",
                      "1BcD2Fgh_consensus",
                      ""]
        test_output = ["AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-34567-89",
                       "AF-12-3456-89",
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "AF-12-3456-89", 
                       "12345678", 
                       "ABCDEFGH", 
                       "ABCDEFGH",
                       "1BCD2FGH",
                       "1BCD2FGH",
                       ""] 
        fail = False 
        i = 0
        for input, output in zip(test_input, test_output):
            try:
                self.assertEqual(phylogeny.process_sample_name(input), output)
            except AssertionError as e:
                i += 1
                fail = True
                print(f"Test failure {i}: ", e)
        if fail: 
            print(f"{i} test failures")
            raise AssertionError
        
    @mock.patch("btbphylo.phylogeny.process_sample_name")
    def test_post_process_snps_df(self, mock_process_sample_name):
        mock_process_sample_name.return_value = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
        test_input_matrix = pd.DataFrame({"A":[0]*10,
                                          "B":[0]*10,
                                          "C":[0]*10,
                                          "D":[0]*10,
                                          "E":[0]*10,
                                          "F":[0]*10,
                                          "G":[0]*10,
                                          "H":[0]*10,
                                          "I":[0]*10,
                                          "J":[0]*10})
        test_input_matrix.index = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
        test_output_matrix = pd.DataFrame({"a":[0]*10,
                                           "b":[0]*10,
                                           "c":[0]*10,
                                           "d":[0]*10,
                                           "e":[0]*10,
                                           "f":[0]*10,
                                           "g":[0]*10,
                                           "h":[0]*10,
                                           "i":[0]*10,
                                           "j":[0]*10})
        test_output_matrix.index = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
        nptesting.assert_array_equal(phylogeny.post_process_snps_df(test_input_matrix), test_output_matrix)
