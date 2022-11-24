# **btb-phylo**

[![APHA-CSU](https://circleci.com/gh/APHA-CSU/btb-phylo.svg?style=svg)](https://app.circleci.com/pipelines/github/APHA-CSU)

`btb-phylo` is APHA software that provides tools for performing phylogeny on processed bovine TB WGS data.

It is also used in production for producing snp matricies to serve the ViewBovine APHA app.

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
./btb-phylo.sh path/to/results/directory path/to/consensus/directory -c path/to/config/json -j 1 --with-docker 
```

This will download the latest docker image from [DockerHub](https://hub.docker.com/r/aphacsubot/btb-phylo) and run the full `btb-phylo` pipeline. Consensus files are downloaded from `s3-csu-003` and a snp-matrix is built using a single thread. 

- `path/to/results/directory` is an absolute output path to the local directory for storing results; 
- `path/to/consensus/directory` is an absolute output path to a local directory where consensus sequences are downloaded; 
- `path/to/config/json` is an absolute path to the [configuration file](#config-file), in `.json` format, that specifies filtering criteria for including samples;
- `-j` is an optional argument setting the number of threads to use for building snp matricies. If omitted it defaults to the number of available CPU cores.

**By default the results directory will contain:**
```
.
├── metadata
│   ├── all_wgs_samples.csv
│   ├── deduped_wgs.csv
│   ├── filters.json
│   ├── metadata.json
│   └── passed_wgs.csv
├── multi_fasta.fas
├── snps.csv
└── snps.fas
```
- `all_wgs_samples.csv`: a csv file containing metadata for all WGS samples in `s3-csu-003`;
- `deduped_wgs.csv`: a copy of `all_wgs_samples.csv` with duplicate submissions removed;
- `filters.json`: a `.json` file describing the filters used for choosing samples;
- `metadata.json`: a `.json` containing metadata for a `btb-phylo` run;
- `passed_wgs.csv`: a copy of `deduped_wgs.csv` after filtering, i.e. WGS metadata for all samples included in phylogeny;
- `multi_fasta.fas`: a fasta file containing consensus sequences for all samples included in the results;
- `snps.fas`: a fasta file containing consensus sequences for all samples included in the results, where only snp sites are retained;
- `snps.csv`: a snp matrix

### Test with an example configuration file
```
./btb-phylo.sh ~/results ~/consensus -c $PWD/example_config.json -j 1 --with-docker 
```
This will run the full pipeline inside a docker container with 4 samples, downloading consensus sequences to `~/consensus` and saving the results to `~/results`.

The final output should be:
```
This is snp-dists 0.8.2
Will use 4 threads.
Read 4 sequences of length 831
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
python setup.py install
```
4. Install software dependencies:
```
sudo apt update
bash ./install/install.bash
```
`./install/install.bash` will install the following dependencies:
- [`snp-sites`](https://github.com/sanger-pathogens/snp-sites) (installed with `apt`)
- [`snp-dists`](https://github.com/tseemann/snp-dists) (installed from source to `~/biotools`, with symlink in `/usr/local/bin`)
- [`megacc`](https://megasoftware.net/) (installed with `apt` from `.deb` file) 

### Test with an example configuration file
```
./btb-phylo.sh ~/results ~/consensus -c $PWD/example_config.json -j 1
```
This will run the full pipeline locally with 4 samples, downloading consensus sequences to `~/consensus` and saving the results to `~/results`.

The final output should be:
```
This is snp-dists 0.8.2
Will use 4 threads.
Read 4 sequences of length 831
```
## <a name="pipe-dets"></a> Pipeline details

The full pipeline consists of six main stages:
1. Updating a local `.csv` that contains metadata for every processed APHA bovine-TB sample. The default path of this file is `./all_wgs_samples.csv`. When new samples are available in `s3-csu-003` this file is updated with new samples only.
2. Removing duplicate WGS submissions. Multiple samples may exist for a given submission, generally due to poor quality data or inconclusive outcomes. This stage chooses one sample from each submission.
3. Filtering the samples by a set of criteria defined in either the [configuration file](#config-file) or a set of command line arguments. The metadata file for filtered samples is saved in the results directory. 
4. "Consistifying" the samples with cattle and movement data. Designed for use with ViewBovine, this removes samples from WGS, cattle and movement datasets that are not common to all three datasets.
5. Downloading consensus sequences for the filtered sample set from `s3-csu-003`. If a consistent directory is used for storing consensus sequences, then only new samples will be downloaded.
6. Performing phylogeny: Detecting snp sites using `snp-sites`, building a snp matrix using `snp-dists` and optionally building a phylogentic tree using `megacc`.

<img src="https://user-images.githubusercontent.com/10742324/200572223-39b10c57-88ff-43ab-83e7-c6272acb4f70.png" width=650, alt="centered image">

## Using the software

Stages 1-6 in [pipeline detials](#pipe-dets) can be run in isolation or combination via a set of sub-commands.

### `python btb_phylo.py -h` (help)

```
usage: btb-phylo [-h] {update_samples,filter,de_duplicate,consistify,phylo,full_pipeline,ViewBovine} ...

positional arguments:
  {update_samples,filter,de_duplicate,consistify,phylo,full_pipeline,ViewBovine}
                        sub-command help
    update_samples      updates a local copy of "all_wgs_samples" metadata .csv file
    filter              filters wgs_samples.csv file
    de_duplicate        removes duplicated wgs samples from wgs_samples.csv
    consistify          removes wgs samples that are missing from cattle and movement data (metadata warehouse)
    phylo               performs phylogeny
    full_pipeline       runs the full phylogeny pipeline: updates full samples summary, filters samples and performs
                        phylogeny
    ViewBovine          runs phylogeny with default settings for ViewBovine

optional arguments:
  -h, --help            show this help message and exit
```

**Get full list of optional arguments for any sub-command:**
```
python btb_phylo.py sub-command -h
```
- `sub-command` is one of `update_samples`, `filter`, `de_duplicate`, `consistify`, `phylo`, `full_pipeline`, `ViewBovine`.

### Common usage patterns

**Running the full pipeline**

- on all pass samples
```
python btb_phylo.py full_pipeline path/to/results/directory path/to/consensus/directory --Outcome Pass
```

- on all pass samples + "consistified" with cattle and movement data
```
python btb_phylo.py full_pipeline path/to/results/directory path/to/consensus/directory --Outcome Pass --cat_mov_path path/to/folder/with/cattle/and/movement/csvs
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
python btb_phylo.py full_pipeline path/to/results/directory path/to/consensus/directory --config path/to/config/file --build_tree
```

Other common optional arguments are:
- `--download_only`: optional switch to download consensus sequences without doing phylogeny
- `-j`: the number of threads to use with `snp-dists`; default is 1

## Production - serving ViewBovine app

`btb-phylo` provides a snp-matrix for ViewBovine APHA. Details of the ViewBovine phylogeny dataflow and sample selection are provided in [ViewBovineDataFlow.md]((https://github.com/APHA-CSU/btb-phylo/blob/main/ViewBovineDataFlow.md)) 

Updating the snp-matrix is triggered manually and should be run either weekly or on arrival of new processed WGS data.

### Updating the snp-matrix

1. Mount FSx drive, `fsx-ranch-017`;
2. Mount FSx drive, `fsx-042`;
2. Run the following command; 
```
./btb-phylo.sh {fsx-017_path}/ViewBovine/app/prod {fsx-042_path}/share/ViewBovine_consensus --meta_path {fsx-017_path}/ViewBovine/app/raw --with-docker
```
**By default the results directory will contain:**
```
.
├── metadata
│   ├── CladeInfo.csv
│   ├── all_wgs_samples.csv
│   ├── cattle.csv
│   ├── consistified_wgs.csv
│   ├── deduped_wgs.csv
│   ├── filters.json
│   ├── metadata.json
│   ├── movement.csv
│   ├── passed_wgs.csv
│   └── report.csv
├── multi_fasta.fas
├── snps.csv
└── snps.fas
```
This will use predefined filtering criteria to download new samples to `fsx-017`, consistify the samples with cattle and movement data and update the snp-matrix on `fsx-017`. 

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

See example, [example_config.json](https://github.com/APHA-CSU/btb-phylo/blob/main/example_config.json), which includes a selection of 6 samples if; they have `pcMapped` > 95%; are in the B6-84, B1-11, B6-11, or B3-11 clades; are either `BritishbTB` or `nonBritishbTB`; and have a maximum `Ncount` of 56000.

To perform phylogeny without any filters, simply omit the `-c` option.
