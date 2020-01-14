

import subprocess
import argparse
import sys
import os.path
import json
import pandas as pd


class Error (Exception): pass

def get_arguments():
    '''
    Parse the command line arguments.
    '''
    parser = argparse.ArgumentParser(description='Parses CSV output by RAMPART and runs analysis step on all barcoded samples')

    main_group = parser.add_argument_group('Main options')
    main_group.add_argument('-p', '--protocol', dest='protocol', required=True,
                            help='Path to RAMPART protocol directory')
    main_group.add_argument('-q', '--pipeline', dest='pipeline', default=None,
                            help='Name of pipeline to run')

    run_group = parser.add_argument_group('Run configuration options')
    run_group.add_argument('-r', '--run_configuration', dest='run_configuration', default='./run_configuration.json')
    run_group.add_argument('-c', '--csv', dest='csv', default='./barcodes.csv')
    run_group.add_argument('-t', '--threads', dest='threads', default=1, type=int)

    run_group.add_argument('remainder', nargs=argparse.REMAINDER)

    args = parser.parse_args()

    return args


def syscall(command, allow_fail=False):
    print(command)
    completed_process = subprocess.run(command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
    if (not allow_fail) and completed_process.returncode != 0:
        print('Error running this command:', command, file=sys.stderr)
        print('Return code:', completed_process.returncode, file=sys.stderr)
        print('\nOutput from stdout:', completed_process.stdout, sep='\n', file=sys.stderr)
        print('\nOutput from stderr:', completed_process.stderr, sep='\n', file=sys.stderr)
        raise Error('Error in system call. Cannot continue')
    print(completed_process.stdout)
    return completed_process

def find_pipeline(protocol_path, pipeline_name, pipeline_dict):
    pipeline_json = protocol_path + "/rampart/pipelines.json"
    if not os.path.exists(pipeline_json):
        sys.exit(
            'Error: %s does not exist. Does the protocols directory have the correct format?' %pipeline_json)

    snakemake = protocol_path + "/rampart/"

    with open(pipeline_json) as json_file:
        pipelines = json.load(json_file)
        assert pipeline_name is not None or len(pipelines) == 1
        if pipeline_name is None:
            pipeline_name = list(pipelines.keys())[0]

        assert pipeline_name in pipelines
        pipeline_dict.update(pipelines[pipeline_name])

        assert "path" in pipelines[pipeline_name]
        snakemake += pipelines[pipeline_name]["path"] + "/Snakefile"
        print(snakemake)
        if not os.path.exists(snakemake):
            sys.exit(
                'Error: %s does not exist. Does the protocols directory have the correct format?' % snakemake)

        pipeline_dict["path"] = snakemake
        if "config_file" in pipeline_dict:
            pipeline_dict["config_file"] = pipeline_dict["path"].replace("Snakefile", pipeline_dict["config_file"])

    return pipeline_dict

def load_run_configuration(run_configuration):
    if not os.path.exists(run_configuration):
        sys.exit(
            'Error: %s does not exist. If not in current working directory, please specify the filepath with '
            '--run_configuration parameter' %run_configuration)
    with open(run_configuration) as json_file:
        config = json.load(json_file)
        print(config)

    sample_dict = {}
    if "samples" in config:
        for sample in config["samples"]:
            sample_dict[sample["name"]] = sample["barcodes"]
    print(sample_dict)
    return config, sample_dict

def csv_to_sample_dict(csv_file):
    csv = pd.read_csv(csv_file)
    sample_dict = {}

    for i,row in csv.iterrows():
        sample, barcode = row['samples'], row['barcodes']
        if sample not in sample_dict:
            sample_dict[sample] = []
        sample_dict[sample].append(barcode)
    print(sample_dict)
    return sample_dict

def update_sample_dict_with_csv(csv_file, sample_dict):
    if os.path.exists(csv_file):
        sample_dict = csv_to_sample_dict(csv_file)
    return sample_dict

def sample_dict_to_dict_string(sample_dict):
    sample_strings = ["%s: [%s]" %(sample, ",".join(sample_dict[sample])) for sample in sample_dict]
    dict_string = "'{%s}'" %", ".join(sample_strings)
    print(dict_string)
    return dict_string

def main():
    args = get_arguments()

    pipeline_dict = {
        "path": None,
        "config": None,
        "config_file": None,
        "options": None
    }
    pipeline_dict = find_pipeline(args.protocol, args.pipeline, pipeline_dict)
    print(pipeline_dict)

    config, sample_dict = load_run_configuration(args.run_configuration)
    sample_dict = update_sample_dict_with_csv(args.csv, sample_dict)
    dict_string = sample_dict_to_dict_string(sample_dict)

    command_list = ['snakemake', '--snakefile', pipeline_dict["path"], "--cores", args.threads]
    if pipeline_dict["config_file"] is not None:
        command_list.extend(["--configfile", pipeline_dict["config_file"]])
    command_list.extend(["--config samples=%s" %dict_string, "basecalled_path=%s" %config["basecalledPath"]])
    command_list.extend(args.remainder)
    command = ' '.join(command_list)
    print(command)
    #syscall(command)

if __name__ == '__main__':
    main()
