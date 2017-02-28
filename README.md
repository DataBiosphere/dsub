### Disclaimer

This is not an official Google product.

# Simple Docker batch jobs on Google Compute Engine (GCE)

## Overview

Provides a command-line interface for submitting script-based jobs to a
job execution engine. Jobs are currently backed by the Google Genomics
Pipelines API.

`dsub` provides two modes of execution:

* launch a single job where parameters are specified on the command-line.
* launch a batch of jobs where parameters are specified in a tab-separated
(TSV) file.

The former is largely intended for enabling iterative development of the job
methods. The latter enables you to ramp up execution from a single input to
a batch.

### Command-line execution

Examples:

    dsub \
      --project my-cloud-project \
      --logging gs://mybucket/mylogs/vcfstats-sample1.log \
      --input gs://mybucket/mypath/sample1.vcf \
      --output gs://mybucket/mypath/sample1.stats.txt \
      my-vcfstats-script.py

    dsub \
      --project my-cloud-project \
      --logging gs://mybucket/mylogs/bamstats-sample1.log \
      --input gs://mybucket/mypath/sample1.bam \
      --output gs://mybucket/mypath/sample1.stats.txt \
      --image_name quay.io/collaboratory/dockstore-tool-bamstats \
      my-bamstats-wrapper.sh

Commands submitted by dsub will run a Docker task (on a GCE VM) where
the specified input files have been automatically localized to:

    /mnt/data/input/gs

In the above examples, the input files would be localized to:

    /mnt/data/input/gs/mybucket/mypath/

On successful completion of the Docker task, the output files will be
automatically de-localized from:

    /mnt/data/output/gs

In the above examples, the output files would be de-localized from:

    /mnt/data/output/gs/mybucket/mypath/

Support for wildcards *on filenames* is available for both inputs and outputs.
For example:

    --input gs://mybucket/mypath/*.bam
    --output gs://mybucket/mypath/*.bam.bai

#### Recursive copy

Support for recursive copy is available for inputs and outputs.

For inputs, this allows you to pull an entire tree of inputs from
Google Cloud Storage (GCS), for example:

    dsub ... \
      --input-recursive INPUT_PATH=gs://bucket/path

Your pipelines script will get a variable INPUT_PATH, which contains the
on-disk location, `/mnt/data/input/gs/bucket/path`.

For outputs, this allows you to generate output with subdirectories and
dsub will recursively copy your output to GCS. For example:

    dsub ... \
      --output-recursive OUTPUT_PATH=gs://bucket/path

Your pipelines script will get a variable OUTPUT_PATH, which contains the
on-disk location, `/mnt/data/output/gs/bucket/path`.

As a convenience, if a recursive input or output parameter is passed, dsub will
automatically install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/)
at runtime (before your script executes). For large jobs, you are encouraged
to install gcloud in your Docker image when it is built.

#### Unsupported path formats:

* GCS recursive wildcards (**) are not supported
* Wildcards in the middle of a path are not supported
* Output parameters to a directory are not supported, instead:
  * use an explicit wildcard on the filename (such as `gs://mybucket/mypath/*`)
  * use the recursive copy feature

#### Script parameters

If your Docker task script needs to refer to inputs or outputs explicitly,
names can be provided for the input and output parameters. These
parameters will be available as environment variables in the Docker container.

For example:

    --input OLD_VCF=gs://mybucket/mypath1/sample.vcf \
    --input NEW_VCF=gs://mybucket/mypath2/sample.vcf \
    --output OUTPUT=gs://mybucket/mypath/*

will result in the following environment variables being set:

    OLD_VCF=/mnt/data/input/gs/mypath1/sample.vcf
    NEW_VCF=/mnt/data/input/gs/mypath2/sample.vcf
    OUTPUT=/mnt/data/output/gs/mypath/*

To pass simple environment variables to your jobs, use the `--env` parameter:

    --env SAMPLE_ID=NA12878 \
    --env SAMTOOLS_TASK=index \

Note that each of the `--env`, `--input`, and `--output` parameters supports
a single value for a flag, multiple space-separated values for a single flag,
or multiple flags. For example:

    --env NAME1=VALUE1 NAME2=VALUE2

or

    --env NAME1=VALUE1 \
    --env NAME2=VALUE2

### TSV file batch execution

A TSV file can be used to specify environment variables, input, and output
parameters for multiple jobs.

The first line of the TSV file specifies the names and types of the parameters.
Just as on the command-line, inputs and outputs can be anonymous or named:

    --env SAMPLE_ID<tab>--input<tab>--output

or:

    --env SAMPLE_ID<tab>--input VCF_FILE<tab>--output OUTPUT_PATH

The first line also supports bare-word variables which are treated as
the names of environment variables.

### Job Control

Each job submitted by dsub is given a set of metadata values which can be
utilized for job identification and job control.

The metadata associated with each job includes:

* `job-name`: defaults to the name of the *script* file, can be explicitly set
  with the --name parameter
* `user-id`: the `USER` environment variable value
* `job-id`: takes the form `<job-name>--<userid>--<timestamp>` where the
  `job-name` portion is truncated at 10 characters and the `timestamp`
  portion is out to hundredths of a second and of the form `YYMMDD-HHMMSS-XX`
* `task-id`: if the job is a "table job", each task gets a sequential value
  of the form "task-*n*" where *n* is 1-based.

Note that each metadata element must conform to the
[Compute Engine label specification]
(https://cloud.google.com/compute/docs/reference/beta/instances/setLabels#labels).

### View Job Status

The `dstat` script allows for viewing job status:

```bash
dstat --project my-cloud-project
```

with no additional arguments will display a list of running jobs for the USER.

```bash
dstat --project my-cloud-project --job-list <job-id>
```

will display the running job (including tasks).

### Cancel Jobs

The `ddel` script allows for deleting jobs:

```bash
ddel --project my-cloud-project --job-list <job-id>
```

will delete the running job (including tasks).

```bash
ddel --project my-cloud-project \
  --job-list <job-id> --task-list <task-id1> <task-id2>
```

will delete the running tasks.

## Setup

To setup:

-   Setup a virtualenv (optional, but very strongly recommended).

    ```bash
    cd $HOME
    virtualenv dsub_libs
    source dsub_libs/bin/activate
    ```

-   Install dependent libraries

    ```bash
    pip install --upgrade oauth2client==1.5.2 google-api-python-client python-dateutil pytz tabulate
    ```

And then you can run with:

-   `ddel [flags]`
-   `dstat [flags]`
-   `dsub [flags] my_script.sh`


To see the full set of parameters for dsub, dstat, ddel, pass the
`--help` flag.

## Examples

A simple getting started example can be found in the directory:

* examples/[decompress](examples/decompress)
