

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
    parser = argparse.ArgumentParser(description='Parses CSV output by RAMPART and runs analysis step on all barcoded \
                                                  samples')

    main_group = parser.add_argument_group('Main options')
    main_group.add_argument('-p', '--protocol', dest='protocol', required=True,
                            help='Path to RAMPART protocol directory')
    main_group.add_argument('-q', '--pipeline', dest='pipeline', default=None,
                            help='Name of pipeline to run. If there is only one pipeline \
                            in the protocol directory, this parameter is optional')

    run_group = parser.add_argument_group('Run configuration options')
    run_group.add_argument('-d', '--run_directory', dest='run_directory', default='./',
                           help='Path to the directory for this run if it is not in current working directory')
    run_group.add_argument('-r', '--run_configuration', dest='run_configuration', default='run_configuration.json',
                          help='Path to the run_configuration.json file relative to the run directory')
    run_group.add_argument('-c', '--csv', dest='csv', default='barcodes.csv',
                          help='Path to the CSV file containing a samples and barcodes column if this information is \
                          not provided in the run_configuration.json file. Path should be relative to the run \
                          directory. Updates the run_configuration information if both are provided')
    run_group.add_argument('-i', '--basecalled_path', dest='basecalled_path', default=None,
                           help='Path to the basecalled directory if this information is \
                              not provided in the run_configuration.json file. Path should be relative to the run \
                              directory. Updates the run_configuration information if both are provided')
    run_group.add_argument('--fast5_path', dest='fast5_path', default=None,
                           help='Path to the directory containing raw fast5 files. If this information is \
                              not provided in the run_configuration.json file. Necessary if guppy demultiplexing is used\
                              after a sequencing run with live basecalling enabled.')
    run_group.add_argument('-t', '--threads', dest='threads', default=1, type=int,
                          help='Number of cores to run snakemake with')
    run_group.add_argument('-n', '--dry_run', dest='dry_run', action="store_true",
                           help='Make this a snakemake dry run')

    run_group.add_argument('remainder', nargs=argparse.REMAINDER,
                          help='String of key=value pairs to override snakemake config parameters with')

    args = parser.parse_args()

    # strip trailing / from paths
    args.protocol = args.protocol.rstrip("/")
    args.run_directory = args.run_directory.rstrip("/")

    # dummy handle if the run_configuration or barcodes csv are given as absolute paths
    if not args.run_directory.startswith("/"):
        args.run_directory = os.path.abspath(args.run_directory)
        print("Using run directory %s" %args.run_directory)

    if not args.run_configuration.startswith("/"):
        args.run_configuration = "%s/%s" % (args.run_directory, args.run_configuration)
        print("Using run configuration %s" % args.run_configuration)

    if not args.csv.startswith("/"):
        args.csv = "%s/%s" % (args.run_directory, args.csv)
        print("Using csv %s" % args.csv)

    return args


def syscall(command, allow_fail=False):
    print(command)

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               universal_newlines=True)

    # Poll process.stdout to show stdout live
    while True:
        output = process.stdout.readline().rstrip()
        if process.poll() is not None:
            break
        if output:
            print(output)
    return_code = process.poll()

    if (not allow_fail) and return_code != 0:
        print('Error running this command:', command, file=sys.stderr)
        print('Return code:', return_code, file=sys.stderr)
        raise Error('Error in system call. Cannot continue')
    return process

def find_pipeline(protocol_path, pipeline_name, pipeline_dict):
    if not os.path.exists(protocol_path):
        sys.exit(
            'Error: Protocol path %s does not exist.' %protocol_path)

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

        if "path" in pipelines[pipeline_name]:
            snakemake += pipelines[pipeline_name]["path"] + "/"
        snakemake += "Snakefile"
        #print(snakemake)
        if not os.path.exists(snakemake):
            sys.exit(
                'Error: %s does not exist. Does the protocols directory have the correct format?' % snakemake)

        pipeline_dict["path"] = snakemake
        if "config_file" in pipeline_dict:
            pipeline_dict["config_file"] = pipeline_dict["path"].replace("Snakefile", pipeline_dict["config_file"])

    return pipeline_dict

def load_run_configuration(run_configuration_path):
    config = {}
    sample_dict = {}
    if not os.path.exists(run_configuration_path):
        print("No run configuration JSON at %s" %run_configuration_path)
        return config, sample_dict
    with open(run_configuration_path) as json_file:
        config = json.load(json_file)
        #print(config)

    if "samples" in config:
        for sample in config["samples"]:
            sample_dict[sample["name"]] = sample["barcodes"]
        del config["samples"]
    #print(sample_dict)
    return config, sample_dict

def csv_to_sample_dict(csv_file):
    csv = pd.read_csv(csv_file)
    sample_dict = {}

    sample_column_names = [s for s in ['samples', 'sample', 'Samples', 'Sample', 'SAMPLES', 'SAMPLE'] if s in csv.columns.values]
    barcode_column_names = [s for s in ['barcodes', 'barcode', 'Barcodes', 'Barcode', 'BARCODES', 'BARCODE'] if s in csv.columns.values]
    if len(sample_column_names) < 1:
        sys.exit("Error: barcodes CSV file does not have a column header for sample/samples")
    if len(barcode_column_names) < 1:
        sys.exit("Error: barcodes CSV file does not have a column header for barcode/barcodes")

    for i,row in csv.iterrows():
        sample, barcode = row[sample_column_names[0]], row[barcode_column_names[0]]
        if sample not in sample_dict:
            sample_dict[sample] = []
        sample_dict[sample].append(barcode)
    #print(sample_dict)
    return sample_dict

def update_sample_dict_with_csv(csv_path, sample_dict):
    #print("Looking for barcodes CSV at %s?" %csv_path)
    if os.path.exists(csv_path):
        print("Update sample_dict with %s" %csv_path)
        sample_dict = csv_to_sample_dict(csv_path)

    if sample_dict == {}:
        print(
            'Warning: no valid sample to barcode map given. Assuming no barcoding. Otherwise this should be provided '
            'either in the run configuration JSON or in a CSV file specified with the --csv parameter, and either '
            'given as an absolute path or relative to the run directory.')

    return sample_dict

def update_config_with_basecalled_path(run_directory, config, basecalled_path):
    if "basecalledPath" not in config:
        config["basecalledPath"] = None
    elif not config["basecalledPath"].startswith("/"):
            config["basecalledPath"] = "%s/%s" %(run_directory, config["basecalledPath"])

    if basecalled_path is not None:
        print("Updating basecalled_path from command line")
        if not basecalled_path.startswith("/"):
            basecalled_path = "%s/%s" %(run_directory, basecalled_path)
        config["basecalledPath"] = basecalled_path

    if config["basecalledPath"] is None:
        sys.exit(
            'Error: no valid basecalledPath given. This should be provided either in the run configuration JSON or'
            'using the --basecalledPath parameter. It should be specified either as an absolute path or relative to'
            'the run directory.')
    elif not os.path.exists(config["basecalledPath"]):
        sys.exit(
            'Error: Basecalled path %s is invalid. This should be provided either in the run configuration JSON or'
            'using the --basecalledPath parameter. It should be specified either as an absolute path or relative to'
            'the run directory.' %config["basecalledPath"])

    return config

def update_config_with_fast5_path(run_directory, config, fast5_path):
    if "fast5Path" not in config:
        config["fast5Path"] = None
    elif not config["fast5Path"].startswith("/"):
            config["fast5Path"] = "%s/%s" %(run_directory, config["fast5Path"])

    if fast5_path is not None:
        print("Updating fast5_path from command line")
        if not fast5_path.startswith("/"):
            fast5_path = "%s/%s" %(run_directory, fast5_path)
        config["fast5Path"] = fast5_path

    return config

def sample_dict_to_dict_string(sample_dict):
    sample_strings = ["%s: [%s]" %(sample, ",".join(sample_dict[sample])) for sample in sample_dict]
    dict_string = "'{%s}'" %", ".join(sample_strings)
    #print(dict_string)
    return dict_string

def generate_command(protocol, pipeline, run_directory, run_configuration, basecalled_path, fast5_path, csv, threads, remainder,
                     dry_run=False):
    pipeline_dict = {
        "path": None,
        "config": None,
        "config_file": None,
        "options": None
    }
    pipeline_dict = find_pipeline(protocol, pipeline, pipeline_dict)
    # print(pipeline_dict)

    config, sample_dict = load_run_configuration(run_configuration)
    config = update_config_with_basecalled_path(run_directory, config, basecalled_path)
    config = update_config_with_fast5_path(run_directory, config, fast5_path)
    sample_dict = update_sample_dict_with_csv(csv, sample_dict)


    command_list = ['snakemake', '--snakefile', pipeline_dict["path"], "--cores", str(threads),
                    "--rerun-incomplete", "--nolock"]
    if dry_run:
        command_list.append("--dry-run")

    if pipeline_dict["config_file"] is not None:
        command_list.extend(["--configfile", pipeline_dict["config_file"]])
    if sample_dict != {}:
        dict_string = sample_dict_to_dict_string(sample_dict)
        command_list.extend(["--config samples=%s" % dict_string])
    command_list.extend(["basecalled_path=\"%s\"" % config["basecalledPath"]])
    if pipeline_dict["config"] is not None:
        command_list.append(pipeline_dict["config"])
    command_list.extend(remainder)
    command = ' '.join(command_list)
    return command

def main():
    args = get_arguments()

    command = generate_command(args.protocol, args.pipeline, args.run_directory, args.run_configuration,
                               args.basecalled_path, args.fast5_path, args.csv, args.threads, args.remainder, args.dry_run)
    syscall(command)

if __name__ == '__main__':
    main()
