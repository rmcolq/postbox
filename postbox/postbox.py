import pandas as pd
import subprocess
import argparse
import sys

class Error (Exception): pass

def get_arguments():
    '''
    Parse the command line arguments.
    '''
    parser = argparse.ArgumentParser(description='Parses CSV output by RAMPART and runs analysis step on all barcoded samples')

    main_group = parser.add_argument_group('Options')
    main_group.add_argument('-c', '--csv', dest='csv', required=True,
                            help='CSV file output by RAMPART containing columns for barcodes and samples (required).')
    main_group.add_argument('-s', '--snakefile', dest='snakefile', default='rampart/pipelines/analyse_all/Snakefile')
    main_group.add_argument('remainder', nargs=argparse.REMAINDER)

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

def csv_to_dict_string(csv_file):
    csv = pd.read_csv(csv_file)
    sample_dict = {}

    for i,row in csv.iterrows():
        sample, barcode = row['samples'], row['barcodes']
        if sample not in sample_dict:
            sample_dict[sample] = []
        sample_dict[sample].append(barcode)

    print(sample_dict)
    sample_strings = ["%s: [%s]" %(sample, ",".join(sample_dict[sample])) for sample in sample_dict]
    dict_string = "'{%s}'" %", ".join(sample_strings)
    print(dict_string)
    return dict_string

def main():
    args = get_arguments()
    sample_dict = csv_to_dict_string(args.csv)
    command_list = ['snakemake', '--snakefile', args.snakefile, "--config samples=%s" %sample_dict]
    command_list.extend(args.remainder)
    command = ' '.join(command_list)
    print(command)
    syscall(command)

if __name__ == '__main__':
    main()
