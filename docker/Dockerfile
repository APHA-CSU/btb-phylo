FROM ubuntu:20.04

################## METADATA ##########################

LABEL base.image=ubuntu:20.04
LABEL software="Bovine-TB Phylogeny Pipeline Image"
LABEL about.summary="Bioinformatics Pipeline for post-processing of sequenced Bovine-TB data"
LABEL about.documentation="https://github.com/APHA-CSU/btb-phylo"
LABEL about.tags="Phylogenomics"

################## ARGS #############################

ARG BOVTB_PATH="/btb-phylo/"

################## DEPENDENCIES ######################

# Copy repository
WORKDIR $BOVTB_PATH
COPY ./ ./

# Sudo
RUN apt-get -y update

# Biotools
RUN bash ./docker/install-all-dependencies.bash

################## ENTRY ######################

CMD python build_snp_matrix.py
