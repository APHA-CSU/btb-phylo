import unittest

import pandas as pd
import numpy.testing as nptesting

import btbphylo.consistify as consistify

class TestConsistify(unittest.TestCase):
    def test_consistify(self):
        # test input
        test_wgs = pd.DataFrame({"Submission": ["A", "B", "C", "D"]})
        test_cattle = pd.DataFrame({"CVLRef": ["B", "C", "D"]})
        test_movements = pd.DataFrame({"SampleName": ["C", "C", "D", "D", "D", "E"],
                                       "Stay_Length": [None, 0, 0, 0, 0, 0]})
        # test output
        test_metadata = {"original_number_of_wgs_records": 4,
                         "original_number_of_cattle_records": 3,
                         "original_number_of_movement_records": 6,
                         "consistified_number_of_wgs_records": 2,
                         "consistified_number_of_cattle_records": 2,
                         "consistified_number_of_movement_records": 5}
        test_wgs_consist = pd.DataFrame({"Submission": ["C", "D"]})
        test_cattle_consist = pd.DataFrame({"CVLRef": ["C", "D"]})
        test_movements_consist = pd.DataFrame({"SampleName": ["C", "C", "D", "D", "D"],
                                               "Stay_Length": [0, 0, 0, 0, 0]})
        # run consistify
        (metadata, wgs_consist, cattle_consist, movements_consist, *_) = \
            consistify.consistify(test_wgs, test_cattle, test_movements)
        # assert output
        nptesting.assert_array_equal(metadata, test_metadata)
        nptesting.assert_array_equal(wgs_consist, test_wgs_consist)
        nptesting.assert_array_equal(cattle_consist, test_cattle_consist)
        nptesting.assert_array_equal(movements_consist, test_movements_consist)
