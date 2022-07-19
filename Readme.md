# **btb-phylo**

[![APHA-CSU](https://circleci.com/gh/APHA-CSU/btb-phylo.svg?style=svg)](https://app.circleci.com/pipelines/github/APHA-CSU)

`btb-phylo` is APHA software that provides tools for performing phylogeny on processed bovine TB WGS data.

The software can run on any linux EC2 instance within DEFRA's scientific computing environment (SCE), with read access to `s3-csu-003`. It downloads consensus files from s3 buckets within the SCE, builds SNP matricies, and builds phylogenetic trees.

The minimal installation simply requires [python3](https://docs.python.org/3.10/) and [Docker](https://www.docker.com/).
