import unittest
from unittest import mock

import pandas as pd
import numpy.testing as nptesting

from btbphylo import update_summary


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
                      "ABcDeFgh",
                      "1BcD2Fgh",
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
        # test case
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

    def test_new_final_out_keys(self):
        # test case
        test_df = pd.DataFrame({"ResultLoc": ["s3://s3-csu-003/v3-2/A/", 
                                              "s3://s3-csu-003/v3-2/A/", 
                                              "s3://s3-csu-003/v3-2/A/", 
                                              "s3://s3-csu-003/v3-2/B/",
                                              "s3://s3-csu-003/v3-2/B/",
                                              "s3://s3-csu-003/v3-2/C/",
                                              "s3://s3-csu-003/v3-2/D/",
                                              "s3://s3-csu-003/v3-2/D/",
                                              "s3://s3-csu-003/v3-2/E/"]})
        # mock get_finalout_s3_keys
        with mock.patch("btbphylo.update_summary.get_finalout_s3_keys") as mock_get_finalout_s3_keys:
            mock_get_finalout_s3_keys.return_value = ["v3-2/A/FinalOut.csv",
                                                      "v3-2/B/FinalOut.csv",
                                                      "v3-2/C/FinalOut.csv",
                                                      "v3-2/D/FinalOut.csv",
                                                      "v3-2/E/FinalOut.csv",
                                                      "v3-2/F/FinalOut.csv",
                                                      "v3-2/G/FinalOut.csv"]
            self.assertEqual(update_summary.new_final_out_keys(test_df), ["v3-2/F/FinalOut.csv",
                                                                          "v3-2/G/FinalOut.csv"])