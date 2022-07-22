# **btb-phylo**

[![APHA-CSU](https://circleci.com/gh/APHA-CSU/btb-phylo.svg?style=svg)](https://app.circleci.com/pipelines/github/APHA-CSU)

`btb-phylo` is APHA software that provides tools for performing phylogeny on processed bovine TB WGS data.

The software can run on any linux EC2 instance within DEFRA's scientific computing environment (SCE), with read access to `s3-csu-003`. It downloads consensus files from s3 from which it builds SNP matricies and phylogenetic trees.

## Running `btb-phylo` - quick start

The full pipeline can be run with [Docker](https://www.docker.com/) and needs only Docker to be installed. 

1. Clone this github repository:
```
git clone https://github.com/APHA-CSU/btb-phylo.git
```
2. Run the following command from inside the cloned repository to run the pipeline inside a docker container:

```
./btb-phylo path/to/results/directory path/to/consensus/directory with-docker -c path/to/config/json -j 1  
```

This will download the latest docker image from [DockerHub](https://hub.docker.com/r/aphacsubot/btb-phylo) and run the full btb-phylo software. Consensus files are downloaded from `s3-csu-003` and a snp-matrix is built using a single thread. 

- `path/to/results/directory` is an output path to the local directory for storing results; 
- `path/to/consensus/directory` is an output path to a local director where consensus sequences are downloaded; 
- `path/to/config/json` is a path to the [configuration file](#config-file), in `.json` format, that specifies filtering criteria for including samples;
- `-j` is an optional argument setting the number of threads to use for building snp matricies. If omitted it defaults to the number of available CPU cores.

By default the results directory will contain:
- `filtered_samples.csv`: a summary csv file containing metadata for all samples included in the results;
- `multi_fasta.fas`: a fasta file containing consensus sequences for all samples included in the results;
- `snps.fas`: a fasta file containing consensus sequences for all samples included results where only snp sites are retained;
- `snp_matrix.tab`

## Local installation
1. Clone this github repository:
```
git clone https://github.com/APHA-CSU/btb-phylo.git
```
2. Install software dependencies:
```
sudo apt update
bash ./install/install.bash
```
This script installs the following dependencies:
- `python3` (insatlled with `apt`)
- `snp-sites` (installed with `apt`)
- `snp-dists` (installed from source to `~/biotools`, with symlink in `/usr/local/bin`)
- `megacc` (installed with `apt` from `.deb` file) 

## Production - serving ViewBovine app

`btb-phylo` provides a snp-matrix for ViewBovine APHA. 

Updating the snp-matrix is triggered manually and should be run either weekly or on arrival of new processed WGS data.

To update the snp-matrix:

1. Mount FSx drive, `fsx-ranch-017`;
2. Run the following command; 
```
./btb-phylo path/to/fsx-017 path/to/fsx-017 with-docker -c $PWD/vb_config.json
```
This will use predefined filtering criteria to download new samples to `fsx-017`, and update the snp-matrix on `fsx-017`. 

### <a name="config-file"></a> Configuration file

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