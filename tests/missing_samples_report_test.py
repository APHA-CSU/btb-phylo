import unittest

import pandas as pd
import numpy.testing as nptesting

import btbphylo.missing_samples_report as missing_samples_report

class TestMissingSamplesReport(unittest.TestCase):
    def test_get_excluded(self):
        # test input
        test_df_deduped = pd.DataFrame({"Submission": ["A", "B"]})
        test_df_included = pd.DataFrame({"Submission": ["A"]})
        # assert outptut
        nptesting.assert_array_equal(missing_samples_report.get_excluded(test_df_deduped,
                                                                         test_df_included),
                                     pd.DataFrame({"Submission": ["B"]}))

    def test_exclusion_reason(self):
        # test input
        test_df_excluded = pd.DataFrame({"Submission": ["A", "B", "C"],
                                         "flag": ["foo", "bar", "baz"],
                                         "Ncount": [3, 2, 1],
                                         "group": ["clade_a", "clade_b", "clade_c"]})
        test_df_clade_info = pd.DataFrame({"maxN": [2, 2, 2]}, 
                                           index=["clade_a", "clade_b", "clade_c"])
        # test output
        test_df_report = pd.DataFrame({"Submission": ["A", "B", "C"],
                                       "flag": ["foo", "bar", "baz"],
                                       "Ncount": ["Fail", "Pass", "Pass"]})
        # assert output
        nptesting.assert_array_equal(missing_samples_report.exclusion_reason(test_df_excluded,
                                                                             test_df_clade_info),
                                     test_df_report)

    def test_missing_data(self):
        # test input
        test_df_report = pd.DataFrame({"Submission": ["complete", 
                                                      "missing_cattle", 
                                                      "missing_movement",
                                                      "missing_cattle_&_movement"],
                                       "flag": ["foo", "bar", "baz", "foobar"],
                                       "Ncount": ["foo", "bar", "baz", "foobar"]})
        test_missing_wgs_samples = {"missing_wgs"}
        test_missing_cattle_samples = {"missing_cattle", "missing_cattle_&_movement"}
        test_missing_movement_samples = {"missing_movement", "missing_cattle_&_movement"}
        # test output
        test_df_report_missing_data = pd.DataFrame({"Submission": ["complete", 
                                                                   "missing_cattle", 
                                                                   "missing_movement", 
                                                                   "missing_cattle_&_movement",
                                                                   "missing_wgs"],
                                                    "flag": ["foo", "bar", "baz", "foobar", None],
                                                    "Ncount": ["foo", "bar", "baz", "foobar", None],
                                                    "wgs_data": [True, True, True, True, False],
                                                    "cattle_data": [True, False, True, False, True],
                                                    "movement_data": [True, True, False, False, True]})
        # assert output
        nptesting.assert_array_equal(missing_samples_report.missing_data(test_df_report,
                                                                         test_missing_wgs_samples,
                                                                         test_missing_cattle_samples,
                                                                         test_missing_movement_samples),
                                     test_df_report_missing_data)

    def test_add_eartag_column(slef):
        # test input
        test_df_report = pd.DataFrame({"Submission": ["A", "B", "C", "D"], 
                                       "cattle_data": [True, True, False, False], 
                                       "movement_data": [True, False, True, False]})
        test_df_cattle = pd.DataFrame({"CVLRef": ["A", "B"], 
                                       "RawEartag2": ["foo", "bar"]})
        test_df_movement = pd.DataFrame({"SampleName": ["A", "C"], 
                                         "StandardEartag": ["foo", "foobar"]})
        # test output
        test_df_report_eartag = pd.DataFrame({"Submission": ["A", "B", "C", "D"], 
                                              "eartag": ["foo", "bar", "foobar", None],
                                              "cattle_data": [True, True, False, False], 
                                              "movement_data": [True, False, True, False]})
        # assert output
        nptesting.assert_array_equal(missing_samples_report.add_eartag_column(test_df_report, 
                                                                              test_df_cattle, 
                                                                              test_df_movement), 
                                     test_df_report_eartag)
