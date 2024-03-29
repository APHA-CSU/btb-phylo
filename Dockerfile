FROM ubuntu:22.04

################## METADATA ##########################

LABEL base.image=ubuntu:22.04
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
RUN bash ./install/docker-install.bash

# Make executable
RUN chmod +x btb-phylo.sh

################## ENTRY ############################

ENTRYPOINT ["./btb-phylo.sh"]
