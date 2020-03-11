import logging
mpl_logger = logging.getLogger('matplotlib')
mpl_logger.setLevel(logging.WARNING)

import os
import unittest
import filecmp

from postbox.postbox import *

this_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(this_dir, 'tests', 'data')

class TestPostbox(unittest.TestCase):
    def setUp(self):
        pass

    def test_syscall_success(self):
        command = "echo hi"
        return_value = syscall(command)
        result = return_value.returncode
        expected = 0
        self.assertEqual(expected, result)

    def test_syscall_fail_handle(self):
        command = "which nonsense"
        return_value = syscall(command, allow_fail=True)
        result = return_value.returncode
        expected = 1
        self.assertEqual(expected, result)

    def test_syscall_fail_exit(self):
        command = "which nonsense"
        with self.assertRaises(Error) as out:
            return_value = syscall(command)
            result = return_value.returncode
            expected = "Error in system call. Cannot continue"
            self.assertEqual(expected, result)

    def test_find_pipeline_protocol_path_does_not_exist(self):
        protocol_path = "idontexist"
        pipeline_name = None
        pipeline_dict = {}
        with self.assertRaises(SystemExit) as out:
            find_pipeline(protocol_path, pipeline_name, pipeline_dict)
            expected = "Error: Protocol path idontexist does not exist."
            self.assertEqual(out.exception, expected)

    def test_find_pipeline_pipeline_json_does_not_exist(self):
        protocol_path = "%s/example_protocol_no_pipeline" %data_dir
        pipeline_name = None
        pipeline_dict = {}
        with self.assertRaises(SystemExit) as out:
            find_pipeline(protocol_path, pipeline_name, pipeline_dict)
            expected = "Error: %s/rampart/pipelines.json does not exist. Does the protocols directory have the correct format?" %protocol_path
            self.assertEqual(out.exception, expected)

    def test_find_pipeline_pipeline_name_is_None(self):
        protocol_path = "%s/example_protocol" %data_dir
        pipeline_name = None
        pipeline_dict = {}
        self.assertRaises(AssertionError, find_pipeline, protocol_path, pipeline_name, pipeline_dict)

    def test_find_pipeline_pipeline_name_is_None_and_two_pipelines(self):
        protocol_path = "%s/example_protocol" %data_dir
        pipeline_name = None
        pipeline_dict = {}
        self.assertRaises(AssertionError, find_pipeline, protocol_path, pipeline_name, pipeline_dict)

    def test_find_pipeline_pipeline_name_not_in_pipelines(self):
        protocol_path = "%s/example_protocol" %data_dir
        pipeline_name = "nonsense"
        pipeline_dict = {}
        self.assertRaises(AssertionError, find_pipeline, protocol_path, pipeline_name, pipeline_dict)

    def test_find_pipeline_snakemake_does_not_exist(self):
        protocol_path = "%s/example_protocol" %data_dir
        pipeline_name = "null"
        pipeline_dict = {}
        snakemake = "%s/Snakemake" %protocol_path
        with self.assertRaises(SystemExit) as out:
            find_pipeline(protocol_path, pipeline_name, pipeline_dict)
            expected = "Error: %s does not exist. Does the protocols directory have the correct format?" % snakemake
            self.assertEqual(out.exception, expected)

    def test_find_pipeline_generate_correct_snakemake_path(self):
        protocol_path = "%s/example_protocol" %data_dir
        pipeline_name = "analysis"
        pipeline_dict = {}
        expected = "%s/rampart/pipelines/analyse_samples/Snakefile" %protocol_path
        pipeline_dict = find_pipeline(protocol_path, pipeline_name, pipeline_dict)
        self.assertEqual(pipeline_dict["path"], expected)

    def test_find_pipeline_update_config_file(self):
        protocol_path = "%s/example_protocol" %data_dir
        pipeline_name = "analysis"
        pipeline_dict = {"config_file": "config.yaml"}
        expected = "%s/rampart/pipelines/analyse_samples/config.yaml" %protocol_path
        pipeline_dict = find_pipeline(protocol_path, pipeline_name, pipeline_dict)
        self.assertEqual(pipeline_dict["config_file"], expected)

    def test_load_run_configuration_path_does_not_exist(self):
        run_configuration_path = "idonotexist.json"
        expected_config = {}
        expected_sample_dict = {}
        config, sample_dict = load_run_configuration(run_configuration_path)
        self.assertEqual(config, expected_config)
        self.assertEqual(sample_dict, expected_sample_dict)

    def test_load_run_configuration_without_samples(self):
        run_configuration_path = "%s/example_run_directory/run_configuration.json" %data_dir
        expected_config = {"title": "MinION_Run_Example", "basecalledPath": "./fastq_pass/"}
        expected_sample_dict = {}
        config, sample_dict = load_run_configuration(run_configuration_path)
        self.assertEqual(config, expected_config)
        self.assertEqual(sample_dict, expected_sample_dict)

    def test_load_run_configuration_with_samples(self):
        run_configuration_path = "%s/example_run_directory/run_configuration_with_samples.json" %data_dir
        expected_config = {"title": "MinION_Run_Example", "basecalledPath": "./fastq_pass/"}
        expected_sample_dict = {"North": ["NB03"], "East": ["NB04"], "South": ["NB05", "NB07"], "Control": ["NB06"]}

        config, sample_dict = load_run_configuration(run_configuration_path)
        self.assertEqual(config, expected_config)
        self.assertEqual(sample_dict, expected_sample_dict)

    def test_csv_to_sample_dict_no_sample(self):
        csv_file = "%s/example_run_directory/barcodes_no_sample.csv" %data_dir
        with self.assertRaises(SystemExit) as out:
            csv_to_sample_dict(csv_file)
            expected = "Error: barcodes CSV file does not have a column header for sample/samples"
            self.assertEqual(out.exception, expected)

    def test_csv_to_sample_dict_no_barcode(self):
        csv_file = "%s/example_run_directory/barcodes_no_barcode.csv" %data_dir
        with self.assertRaises(SystemExit) as out:
            csv_to_sample_dict(csv_file)
            expected = "Error: barcodes CSV file does not have a column header for barcode/barcodes"
            self.assertEqual(out.exception, expected)

    def test_csv_to_sample_dict_success_singular(self):
        csv_file = "%s/example_run_directory/barcodes.csv" %data_dir
        expected_sample_dict = {"North": ["BC01"], "East": ["BC02"], "South": ["BC03"], "West": ["BC04"], "Control": ["BC05"]}
        sample_dict = csv_to_sample_dict(csv_file)
        self.assertEqual(sample_dict, expected_sample_dict)

    def test_csv_to_sample_dict_success_plural(self):
        csv_file = "%s/example_run_directory/barcodes_plural.csv" %data_dir
        expected_sample_dict = {"North": ["BC01"], "East": ["BC02"], "South": ["BC03"], "West": ["BC04"], "Control": ["BC05"]}
        sample_dict = csv_to_sample_dict(csv_file)
        self.assertEqual(sample_dict, expected_sample_dict)

    def test_update_sample_dict_with_csv_success(self):
        csv_path = "%s/example_run_directory/barcodes.csv" % data_dir
        sample_dict = {"North": ["NB03"], "East": ["NB04"], "South": ["NB05", "NB07"], "Control": ["NB06"]}
        expected_sample_dict = {"North": ["BC01"], "East": ["BC02"], "South": ["BC03"], "West": ["BC04"],
                                "Control": ["BC05"]}
        sample_dict = update_sample_dict_with_csv(csv_path, sample_dict)
        self.assertEqual(sample_dict, expected_sample_dict)

    def test_update_sample_dict_with_csv_no_file_but_valid_sample_dict(self):
        csv_path = "%s/example_run_directory/idontexist.csv" % data_dir
        sample_dict = {"North": ["NB03"], "East": ["NB04"], "South": ["NB05", "NB07"], "Control": ["NB06"]}
        expected_sample_dict = {"North": ["NB03"], "East": ["NB04"], "South": ["NB05", "NB07"], "Control": ["NB06"]}
        sample_dict = update_sample_dict_with_csv(csv_path, sample_dict)
        self.assertEqual(sample_dict, expected_sample_dict)

    def test_update_sample_dict_with_csv_no_file_no_valid_sample_dict(self):
        csv_path = "%s/example_run_directory/idontexist.csv" % data_dir
        sample_dict = {}
        with self.assertRaises(SystemExit) as out:
            update_sample_dict_with_csv(csv_path, sample_dict)
            expected = "Error: no valid sample to barcode map given. This should be provided either in the run " \
                       "configuration JSON or in a CSV file specified with the --csv parameter, and either given " \
                       "as an absolute path or relative to the run directory."
            self.assertEqual(out.exception, expected)

    def test_update_config_with_basecalled_path_no_path_provided(self):
        run_directory = ""
        config = {}
        basecalled_path = None
        with self.assertRaises(SystemExit) as out:
            update_config_with_basecalled_path(run_directory, config, basecalled_path)
            expected = "Error: no valid basecalledPath given. This should be provided either in the run " \
                       "configuration JSON or using the --basecalledPath parameter. It should be specified either " \
                       "as an absolute path or relative to the run directory."
            self.assertEqual(out.exception, expected)

    def test_update_config_with_basecalled_path_absolute_input_path(self):
        run_directory = ""
        config = {}
        basecalled_path = "%s/example_run_directory/fastq_pass" %data_dir
        expected = {"basecalledPath": basecalled_path}
        config = update_config_with_basecalled_path(run_directory, config, basecalled_path)
        self.assertEqual(config, expected)

    def test_update_config_with_basecalled_path_relative_input_path(self):
        run_directory = "%s/example_run_directory" %data_dir
        config = {}
        basecalled_path = "fastq_pass"
        expected = {"basecalledPath": "%s/%s" %(run_directory, basecalled_path)}
        config = update_config_with_basecalled_path(run_directory, config, basecalled_path)
        self.assertEqual(config, expected)

    def test_update_config_with_basecalled_path_update_config(self):
        run_directory = "%s/example_run_directory" %data_dir
        config = {"basecalledPath": "nonsense_path"}
        basecalled_path = "fastq_pass"
        expected = {"basecalledPath": "%s/%s" %(run_directory, basecalled_path)}
        config = update_config_with_basecalled_path(run_directory, config, basecalled_path)
        self.assertEqual(config, expected)

    def test_update_config_with_basecalled_path_path_does_not_exist(self):
        run_directory = "%s/example_run_directory/" %data_dir
        config = {}
        basecalled_path = "fastq"
        with self.assertRaises(SystemExit) as out:
            update_config_with_basecalled_path(run_directory, config, basecalled_path)
            expected = "Error: no valid basecalledPath given. This should be provided either in the run " \
                       "configuration JSON or using the --basecalledPath parameter. It should be specified either " \
                       "as an absolute path or relative to the run directory."
            self.assertEqual(out.exception, expected)

    def test_sample_dict_to_dict_string(self):
        sample_dict = {"North": ["NB03"], "East": ["NB04"], "South": ["NB05", "NB07"], "Control": ["NB06"]}
        expected = "'{North: [NB03], East: [NB04], South: [NB05,NB07], Control: [NB06]}'"
        sample_string = sample_dict_to_dict_string(sample_dict)
        self.assertEqual(sample_string, expected)

    # def test_generate_command(self):
    #     protocol = "%s/example_protocol" %data_dir
    #     pipeline = "analysis"
    #     run_directory = "%s/example_run_directory" %data_dir
    #     run_configuration = "run_configuration.json"
    #     basecalled_path = None
    #     csv = "barcodes.csv"
    #     threads = 1
    #     remainder = ""
    #     dry_run = False
    #
    #     command = generate_command(protocol, pipeline, run_directory, run_configuration, basecalled_path, csv, threads,
    #                                remainder, dry_run)
    #     expected = ""
    #     self.assertEqual(command, expected)


