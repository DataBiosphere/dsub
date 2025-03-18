# Split and Process with dsub

`dsub` can make it easy to run multiple tasks in parallel.
Often you want to split an existing data file into multiple shards
and then process each of those shards in parallel.

This example provides a simple illustration of that process.
The [demo_split_process.sh](demo_split_process.sh) script will:

* run a dsub job on a simple text file, splitting it into shards
  * each shard is written as  `--output`
* run a script on each of the shards (as `--input`) in parallel

As written, the example will run locally on your machine
(as you would when putting together a pipeline). It can also
run on Google Cloud with minimal change (delete the --provider line).

## Setup

* Follow the [dsub getting started](../../README.md#getting-started)
instructions.

Since this script uses the `local` backend provider, you will need
to be sure to install `docker`.

## Running the example

Usage:
```
./demo_split_process.sh <inputfile> <local_path_or_gcs_path>
```

### Send output to a local path

```
WORKSPACE=/tmp/split_process
./demo_split_process.sh input.txt "${WORKSPACE}"
ls "${WORKSPACE}/output/"
rm "${WORKSPACE}/temp/*"
```

### Send output to Google Cloud Storage

```
WORKSPACE=gs://mybucket/someprefix
./demo_split_process.sh input.txt "${WORKSPACE}"
gsutil ls "${WORKSPACE}/output/"
gsutil rm "${WORKSPACE}/temp/*"
```
