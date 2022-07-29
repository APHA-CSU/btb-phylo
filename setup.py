from setuptools import setup, find_packages  
from shutil import rmtree

NAME = "btb_phylo"

# setup
setup(name=NAME, 
      version="beta",
      license="MIT",
      url="https://github.com/APHA-CSU/btb-phylo",
      install_requires=['pandas', 'boto3'], 
      packages = find_packages())

# remove build and metadata
rmtree(f"{NAME}.egg-info", ignore_errors=True)
rmtree("dist", ignore_errors=True)
rmtree("build", ignore_errors=True)

