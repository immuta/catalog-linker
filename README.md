# Catalog Linker

This script is used for attempting to manually link any unlinked Immuta data
source to its corresponding external catalog resource. This is done by an exact
match of the data source name to the resource name. If multiple resources are
found, the data source will not be linked, but the names and identifiers of the
resources and the data source they might correspond to will be output to file
for manual review. These results may be found in the `results` directory along
with an example file detailing the schema.

### Configuration

Before using the script, please copy the `example-config.yaml` to `config.yaml`
and modify as necessary. The example file is commented, please follow the
instructions provided within when setting values.

### Building and Running

Before running the script, please run `pip3 install -r requirements.txt`. The
script is simple and can be run with `python3 link.py` after the requirements
have been installed and the configurations have been set.

### Notes
* Please keep in mind that you may have unexpected results if using more than
  one asset type in the configuration. Please only use one asset type unless
  absolutely necessary.
