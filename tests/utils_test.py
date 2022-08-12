import unittest

from btbphylo import utils as utils


class TestUtils(unittest.TestCase):
    def test_extract_submission_no(self):
        # test cases
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
                self.assertEqual(utils.extract_submission_no(input), output)
            except AssertionError as e:
                i += 1
                fail = True
                print(f"Test failure {i}: ", e)
        if fail: 
            print(f"{i} test failures")
            raise AssertionError
