Grakn LDBC SNB Interactive Workload Implementation
==================================================

# Generating the validation data

In order to generate the validation data clone these two git repos:

`git@github.com:ldbc/ldbc_snb_interactive_validation.git`

`git@github.com:graknlabs/ldbc-snb.git`

The validation repo contains the pre-generated csv files used for validation.
The Grakn ldbc repo contains the code for loading the graph.
Unpack the archive `readwrite_neo4j--validation_set.tar.gz` from the validation repo and record the path `$VALIDATION`.
Then the raw data needs to be copied into the Grakn ldbc folder.

`cp -r $VALIDATION/* $GRAKN_LDBC/social_network/`

The following command should be removed from the `runGraknMigrator.sh` script:

`run.sh`

Then the script can be executed with a running instance of the Grakn distribution.

`./runGraknMigrator.sh localhost:4567 snb`

# Running the data validation

In order to confirm that the queries are performing as expected the ldbc provide a validation driver.
This should be run before the benchmark to ensure everything is running as expected.
In order to run the validation you need this additional git repo:

`git@github.com:ldbc/ldbc_driver.git`

This repo contains the driver for executing the queries. The implementations of the driver interfaces are stored in this repository:

`git@github.com:mikonapoli/ldbc-snb-impls.git`

in the `short-query-handlers` branch.