# **btb-phylo**

[![APHA-CSU](https://circleci.com/gh/APHA-CSU/btb-phylo.svg?style=svg)](https://app.circleci.com/pipelines/github/APHA-CSU)

`btb-phylo` is APHA software that provides tools for performing phylogeny on processed bovine TB WGS data.

The software can run on any linux EC2 instance within DEFRA's scientific computing environment (SCE), with read access to `s3-csu-003`. It downloads consensus files from s3 from which it builds SNP matricies and phylogenetic trees.

## Running `btb-phylo` - quick start

The full pipeline can be run with [Docker](https://www.docker.com/) and needs only Docker to be installed. It can be run with the following command:

```
./btb-phylo path/to/results/directory path/to/consensus/directory path/to/config/json with-docker -j 1
```

This will download the latest docker image from [DockerHub](https://hub.docker.com/r/aphacsubot/btb-phylo) and run the full btb-phylo software. Consensus files are downloaded from `s3-csu-003` and a snp-matrix is built using a single thread. 

`path/to/results/directory` is a path to the local directory to where the results should be stored; `path/to/consensus/directory` is the path to a local director to where consensus sequences should be downloaded; `path/to/config/json` is a a path to the configuration `.json` file specifying filtering criteria which describes which samples to be included in phylogeny. `-j` is an optional argument which sets the number of threads to use for building snp matricies, if omitted this will default to the number of available CPU cores.

By default the results directory will contain:
- `filtered_samples.csv`: a summary csv file containing metadata for all samples included in the results.
- `multi_fasta.fas`: a fasta file containing consensus sequences for all samples included in the results.
- `snps.fas`: a fasta file containing consensus sequences for all samples included results where only snp sites are retained.
- `snp_matrix.tab`: a snp-matrix

The config file specifies what filtering criteria should be used to choose samples. It is a json file with the following format:

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

For numerical variables, e.g. `Ncount` and `pcMapped` the criteria should be a maxium and minimum number. For categorical variables, e.g. `Sample` or `flag` (clade) the criteria should be a list of strings. 

For example:

```
{
    "Sample":["AF-12-01663-21", "AF-61-00725-15", "AFT-61-00846-22", "AF-12-02550-18", "16-3828-08-a"],
    "pcMapped":[95, 100],
    "flag":["B6-11"],
    "Ncount":[0, 5500]
}
```