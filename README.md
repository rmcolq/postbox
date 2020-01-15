# postbox
Takes a RAMPART protocol and allows the post-processing pipeline to be run on all or a subset of samples from the command line.

## Installation
Clone and enter the repository using
```
git clone https://github.com/rmcolq/postbox.git
cd postbox
```
then install using either
```
pip install .
```
or
```
python3 setup.py install
python3 setup.py test
```

## Usage
```
usage: postbox [-h] -p PROTOCOL [-q PIPELINE] [-r RUN_CONFIGURATION] [-c CSV]
               [-t THREADS]
               ...

Parses CSV output by RAMPART and runs analysis step on all barcoded samples

optional arguments:
  -h, --help            show this help message and exit

Main options:
  -p PROTOCOL, --protocol PROTOCOL
                        Path to RAMPART protocol directory
  -q PIPELINE, --pipeline PIPELINE
                        Name of pipeline to run. If there is only one pipeline
                        in the protocol directory, this parameter is optional

Run configuration options:
  -r RUN_CONFIGURATION, --run_configuration RUN_CONFIGURATION
                        Path to the run_configuration.json file if it is not
                        in current working directory
  -c CSV, --csv CSV     Path to the CSV file containing a samples and barcodes
                        column if this information is not provided in the
                        run_configuration.json file. Updates the
                        run_configuration information if both are provided
  -t THREADS, --threads THREADS
                        Number of cores to run snakemake with
  remainder             String of key=value pairs to override snakemake config
                        parameters with
```

## Example command
```
postbox -p /path/to/protocol -q analysis -t 10 basecalled_path=/path/to/basecalled/files
```
