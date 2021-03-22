# bifrostlib

![run_tests](https://github.com/ssi-dk/bifrostlib/workflows/run_tests/badge.svg)

![publish_to_pypi](https://github.com/ssi-dk/bifrostlib/workflows/publish_to_pypi/badge.svg)

This image is run given a directory of reads and a metadata.tsv which provides samples expected and 
provided species. The script will then create a sample object for all samples found and a run object 
containing all the sample objects. A script is then created based of the provided pre, per, and post
scripts that can are customized to each install. Running this script will then create 
sample_component objects as each sample will be ran against the provided components in the per 
script
