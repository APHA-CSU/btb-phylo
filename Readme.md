# **btb-phylo**

[![APHA-CSU](https://circleci.com/gh/APHA-CSU/btb-phylo.svg?style=svg)](https://app.circleci.com/pipelines/github/APHA-CSU)

`btb-phylo` is APHA software that provides tools for performing phylogeny on processed bovine TB WGS data.

The software can run on any linux EC2 instance within DEFRA's scientific computing environment (SCE), with read access to `s3-csu-003`. 

It downloads consensus files from `s3` from which it builds SNP matricies and phylogenetic trees.

## Running `btb-phylo` - quick start

The full pipeline can be run with [Docker](https://www.docker.com/) and needs only Docker to be installed. 

1. Clone this github repository:
```
git clone https://github.com/APHA-CSU/btb-phylo.git
```
2. Run the following command from inside the cloned repository to run the pipeline inside a docker container:

```
./btb-phylo path/to/results/directory path/to/consensus/directory -c path/to/config/json -j 1 with-docker 
```

This will download the latest docker image from [DockerHub](https://hub.docker.com/r/aphacsubot/btb-phylo) and run the full `btb-phylo` pipeline. Consensus files are downloaded from `s3-csu-003` and a snp-matrix is built using a single thread. 

- `path/to/results/directory` is an output path to the local directory for storing results; 
- `path/to/consensus/directory` is an output path to a local directory where consensus sequences are downloaded; 
- `path/to/config/json` is a path to the [configuration file](#config-file), in `.json` format, that specifies filtering criteria for including samples;
- `-j` is an optional argument setting the number of threads to use for building snp matricies. If omitted it defaults to the number of available CPU cores.

**By default the results directory will contain:**
- `filtered_samples.csv`: a summary csv file containing metadata for all samples included in the results;
- `multi_fasta.fas`: a fasta file containing consensus sequences for all samples included in the results;
- `snps.fas`: a fasta file containing consensus sequences for all samples included in the results, where only snp sites are retained;
- `snp_matrix.tab`

### Test with an example configuration file
```
./btb-phylo ~/results ~/consensus -c $PWD/config/example_config.json -j 1 with-docker 
```
This will run the full pipeline inside a docker container with 3 samples, downloading consensus sequences to `~/consensus` and saving the results to `~/results`.

The final output should be:
```
Running snp_dists ... 
This is snp-dists 0.8.2
Will use 1 threads.
Read 3 sequences of length 614
```
## Local installation

1. You must have [`python3`](https://www.python.org/) and `python3-pip` installed. Using a virtual environment with either [`venv`](https://docs.python.org/3/library/venv.html) or [`virtualenv`](https://virtualenv.pypa.io/en/stable/installation.html) is recommended:
```
sudo apt install python3
sudo apt install python3-pip
```
2. Clone this github repository:
```
git clone https://github.com/APHA-CSU/btb-phylo.git
```
3. Install required python packages :
```
cd btb-phylo
pip install -r requirements.txt
```
4. Install software dependencies:
```
sudo apt update
bash ./install/install.bash
```
This ./install/install.bash will install the following dependencies:
- [`snp-sites`](https://github.com/sanger-pathogens/snp-sites) (installed with `apt`)
- [`snp-dists`](https://github.com/tseemann/snp-dists) (installed from source to `~/biotools`, with symlink in `/usr/local/bin`)
- [`megacc`](https://megasoftware.net/) (installed with `apt` from `.deb` file) 

### Test with an example configuration file
```
./btb-phylo ~/results ~/consensus -c $PWD/config/example_config.json -j 1
```
This will run the full pipeline locally with 3 samples, downloading consensus sequences to `~/consensus` and saving the results to `~/results`.

The final output should be:
```
Running snp_dists ... 
This is snp-dists 0.8.2
Will use 1 threads.
Read 3 sequences of length 614
```
## <a name="pipe-dets"></a> Pipeline details

The full pipeline consists of four main stages:
1. Updating a local `.csv` that contains metadata for every processed APHA bovine-TB sample. The default path of this file is `./all_samples.csv`. When new samples are available in `s3-csu-003` this file is updated with new samples only.
2. Filtering the samples by a set of criteria defined in either the [configuration file](#config-file) or a set of command line arguments. The metadata file for filtered samples is saved in the results directory. 
3. Downloading consensus sequences for the filtered sample set from `s3-csu-003`. If a consistent directory is used for storing consensus sequences, then only new samples will be downloaded.
4. Performing phylogeny: Detecting snp sites using `snp-sites`, building a snp matrix using `snp-dists` and optionally building a phylogentic tree using `megacc`.

## Using the software

Stages 1-4 in [pipeline detials](#pipe-dets) can be run in isolation or combination via a set of sub-commands.

### `python btb_phylo.py -h` (help)

```
usage: btb-phylo [-h] {update_samples,filter,phylo,update_and_filter,filter_and_phylo,full_pipeline} ...

positional arguments:
  {update_samples,filter,phylo,update_and_filter,filter_and_phylo,full_pipeline}
                        sub-command help
    update_samples      updates a local copy of all sample metadata .csv file
    filter              filters sample metadata .csv file
    phylo               performs phylogeny
    update_and_filter   updates a metadata .csv file and filters samples
    filter_and_phylo    filters samples and performs phylogeny
    full_pipeline       runs the full phylogeny pipeline: updates full samples summary, filters samples and performs phylogeny

optional arguments:
  -h, --help            show this help message and exit
```

**Get full list of optional arguments for any sub-command:**
```
python btb_phylo.py sub-command -h
```
- `sub-command` is one of `update_samples`, `filter`, `phylo`, `update_and_filter`, `filter_and_phylo`, `full_pipeline`.

### Common usage patterns

**Running the full pipeline**

- on all pass samples
```
python btb_phylo.py full_pipeline path/to/results/directory path/to/consensus/directory
```

- filtering with a [configuration file](#config-file)
```
python btb_phylo.py full_pipeline path/to/results/directory path/to/consensus/directory --config path/to/config/file
```

- on a subset of samples
```
python btb_phylo.py full_pipeline path/to/results/directory path/to/consensus/directory --sample_name AFT-61-00846-22 AF-12-02550-18 16-3828-08-a
```

- building a pyhlogentic tree and filtering with a configuration file
```
python btb_phylo.py full_pipeline path/to/results/directory path/to/consensus/directory --config path/to/config/file --tree
```

Other common optional arguments are:
- `--download_only`: optional switch to download consensus sequences without doing phylogeny
- `-j`: the number of threads to use with `snp-dists`; default is 1

## Production - serving ViewBovine app

`btb-phylo` provides a snp-matrix for ViewBovine APHA. 

Updating the snp-matrix is triggered manually and should be run either weekly or on arrival of new processed WGS data.

### Updating the snp-matrix

1. Mount FSx drive, `fsx-ranch-017`;
2. Run the following command; 
```
./btb-phylo path/to/fsx-017 path/to/fsx-017 -c $PWD/config/vb_config.json with-docker
```
This will use predefined filtering criteria to download new samples to `fsx-017`, and update the snp-matrix on `fsx-017`. 

## <a name="config-file"></a> Configuration file

The configuration file specifies which filtering criteria should be used to choose samples. It is a `.json` file with the following format:

```
{
    "parameter_1":[criteria],
    "parameter_2":[criteria],
    .
    .
    .
    "parameter_n":[criteria]
}
```
Each `parameter` key should be one of the following: 

`Sample`, `GenomeCov`, `MeanDepth`, `NumRawReads`, `pcMapped`, `Outcome`, `flag`, `group`, `CSSTested`, `matches`, `mismatches`, `noCoverage`, `anomalous`, `Ncount`, `ResultLoc`, `ID`, `TotalReads`, `Abundance` 

(i.e. the column names in `FinalOut.csv` output from `btb-seq`). 

For numerical variables, e.g. `Ncount` and `pcMapped` the criteria should be a maxium and minimum number. For categorical variables, e.g. `Sample`, `group` (clade) or `flag` the criteria should be a list of strings. 

See example, [example_config.json](https://github.com/APHA-CSU/btb-phylo/blob/dockerize/example_config.json), which includes a selection of 5 samples if; they have `pcMapped` > 95%; are in the B6-11 clade; are either `BritishbTB` or `nonBritishbTB`; and have a maximum `Ncount` of 5500.

To perform phylogeny without any filters, i.e. on all pass samples, simply omit the `-c` option.