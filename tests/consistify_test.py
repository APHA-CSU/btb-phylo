import unittest

import pandas as pd
import numpy.testing as nptesting

import btbphylo.consistify as consistify

class TestConsistify(unittest.TestCase):
    def test_consistify(self):
        # test input
        test_wgs = pd.DataFrame({"Submission": ["A", "B", "C", "D"]})
        test_cattle = pd.DataFrame({"CVLRef": ["B", "C", "D"]})
        test_movement = pd.DataFrame({"SampleName": ["C", "C", "D", "D", "D", "E"]})
        test_wgs_consist = pd.DataFrame({"Submission": ["C", "D"]})
        test_cattle_consist = pd.DataFrame({"CVLRef": ["C", "D"]})
        test_movement_consist = pd.DataFrame({"SampleName": ["C", "C", "D", "D", "D"]})
        # run consistify
        (wgs_consist, cattle_consist, movement_consist, *_) = \
            consistify.consistify(test_wgs, test_cattle, test_movement)
        # assert output
        nptesting.assert_array_equal(wgs_consist, test_wgs_consist)
        nptesting.assert_array_equal(cattle_consist, test_cattle_consist)
        nptesting.assert_array_equal(movement_consist, test_movement_consist)
        # test that dupliactes entires are not removed
        test_wgs = pd.DataFrame({"Submission": ["A", "A"]})
        test_cattle = pd.DataFrame({"CVLRef": ["A"]})
        test_movement = pd.DataFrame({"SampleName": ["A"]})
        test_wgs_consist = pd.DataFrame({"Submission": ["A", "A"]})
        test_cattle_consist = pd.DataFrame({"CVLRef": ["A"]})
        test_movement_consist = pd.DataFrame({"SampleName": ["A"]})
        # run consistify
        (wgs_consist, cattle_consist, movement_consist, *_) = \
            consistify.consistify(test_wgs, test_cattle, test_movement)
        # assert output
        nptesting.assert_array_equal(wgs_consist, test_wgs_consist)
        nptesting.assert_array_equal(cattle_consist, test_cattle_consist)
        nptesting.assert_array_equal(movement_consist, test_movement_consist)


    def test_clade_correction(self):
        # test input
        test_wgs = pd.DataFrame({"Submission": ["A", "B", "C"],
                                 "group": ["A", "B", "C"]})
        test_cattle = pd.DataFrame({"CVLRef": ["A", "B", "C"],
                                    "clade": ["A", "B", "D"]})
        # test output
        test_cattle_corrected = pd.DataFrame({"CVLRef": ["A", "B", "C"],
                                              "clade": ["A", "B", "C"]})
        # run clade_correction
        cattle_corrected = consistify.clade_correction(test_wgs, test_cattle)
        # assert output
        nptesting.assert_array_equal(cattle_corrected, test_cattle_corrected)
