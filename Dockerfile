FROM ubuntu:20.04

################## METADATA ##########################

LABEL base.image=ubuntu:20.04
LABEL software="Bovine-TB Phylogeny Pipeline Image"
LABEL about.summary="Bioinformatics Pipeline for post-processing of sequenced Bovine-TB data"
LABEL about.documentation="https://github.com/APHA-CSU/btb-phylo"
LABEL about.tags="Phylogenomics"

################## DEPENDENCIES ######################

# Copy repository
WORKDIR "/btb-phylo/"
COPY ./ ./

# Sudo
RUN apt-get -y update

# Biotools
RUN bash ./install/install.bash

################## ENTRY ######################

CMD python build_snp_matrix.py
