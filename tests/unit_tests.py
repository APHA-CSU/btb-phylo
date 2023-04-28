import unittest
import argparse

from phylogeny_test import TestPhylogeny
from filter_samples_test import TestFilterSamples
from update_summary_test import TestUpdateSummary
from consistify_test import TestConsistify
from missing_samples_report_test import TestMissingSamplesReport
from de_duplicate_test import TestDeDuplicate
from utils_test import TestUtils


def test_suit(test_objs):
    suit = unittest.TestSuite(test_objs)
    return suit


if __name__ == "__main__":
    phylogeny_test = [TestPhylogeny('test_build_multi_fasta'),
                      TestPhylogeny('test_extract_s3_bucket'),
                      TestPhylogeny('test_match_s3_uri'),
                      TestPhylogeny('test_process_sample_name'),
                      TestPhylogeny('test_post_process_snps_df')]
    filter_samples_test = [TestFilterSamples('test_filter_df'),
                           TestFilterSamples('test_filter_columns_numeric'),
                           TestFilterSamples('test_filter_columns_categorical')]
    de_duplicate_test = [TestDeDuplicate('test_remove_duplicates'),
                         TestDeDuplicate('test_get_indexes_to_remove')]
    update_summary_test = [TestUpdateSummary('test_append_df_wgs'),
                           TestUpdateSummary('test_get_finalout_s3_keys'),
                           TestUpdateSummary('test_extract_s3_key')]
    missing_samples_report_test = [TestMissingSamplesReport('test_get_excluded'),
                                   TestMissingSamplesReport('test_exclusion_reason'),
                                   TestMissingSamplesReport('test_missing_data'),
                                   TestMissingSamplesReport('test_add_eartag_column')]
    consistify_test = [TestConsistify('test_consistify'),
                       TestConsistify('test_clade_correction')]
    utils_test = [TestUtils('test_extract_submission_no')]
    runner = unittest.TextTestRunner()
    parser = argparse.ArgumentParser(description='Test code')
    module_arg = parser.add_argument('--module', '-m', nargs=1,
                                     help="module to test: phylogeny, update_summary, filter_samples, de_duplicate, consistify or uitls'",
                                     default=None)
    args = parser.parse_args()
    if args.module:
        if args.module[0] == 'phylogeny':
            runner.run(test_suit(phylogeny_test))
        elif args.module[0] == 'filter_samples':
            runner.run(test_suit(filter_samples_test))
        elif args.module[0] == 'de_duplicate':
            runner.run(test_suit(de_duplicate_test))
        elif args.module[0] == 'update_summary':
            runner.run(test_suit(update_summary_test))
        elif args.module[0] == 'missing_samples_report':
            runner.run(test_suit(missing_samples_report_test))
        elif args.module[0] == 'consistify':
            runner.run(test_suit(consistify_test))
        elif args.module[0] == 'utils':
            runner.run(test_suit(utils_test))
        else:
            raise argparse.ArgumentError(module_arg,
                                         "Invalid argument. Please use phylogeny, update_summary, filter_samples, consistify or utils")
    else:
        unittest.main(buffer=True)
