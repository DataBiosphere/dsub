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
      --logging gs://mybucket/mylogs/bamstats-sample1.log \
      --image quay.io/collaboratory/dockstore-tool-bamstats \
      --script my-bamstats-wrapper.sh \
      --input INPUT_FILE=gs://mybucket/mypath/sample1.bam \
      --output OUTPUT_FILE=gs://mybucket/mypath/sample1.stats.txt

This command submitted by dsub will run a Docker task where the specified
input files have been automatically localized to disk.
For example, the `INPUT_FILE` will be localized to:

    /mnt/data/input/gs/mybucket/mypath/sample1.bam

A Docker container will be created from the image at
`quay.io/collaboratory/dockstore-tool-bamstats` and the local script
`my-bamstats-wrapper.sh` will be executed.

On successful completion of the Docker task, the output file will be
automatically de-localized from:

    /mnt/data/output/gs/mybucket/mypath/sample1.stats.txt

More support is available for input and output handling such as:

  * wildcards on filenames
  * recursive directory copying

See the [Input/Output docs](docs/input_output.md) for more details.

#### Script variables

In the above example, your Docker script will receive environment variables
`INPUT_FILE` and `OUTPUT_FILE` set automatically.

To pass simple, non-file, values to your jobs, use the `--env` parameter:

    --env SAMPLE_ID=NA12878 \
    --env SAMTOOLS_TASK=index \

Note that each of the `--env`, `--input`, and `--output` parameters supports
a multiple space-separated values for a single flag, or multiple flags.
For example:

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
dstat --project my-cloud-project --jobs <job-id>
```

will display the running job (including tasks).

### Cancel Jobs

The `ddel` script allows for deleting jobs:

```bash
ddel --project my-cloud-project --jobs <job-id>
```

will delete the running job (including tasks).

```bash
ddel --project my-cloud-project \
  --jobs <job-id> --tasks <task-id1> <task-id2>
```

will delete the running tasks.

## Setup

To setup:

- Follow the Google Genomics Pipelines API
[Get Ready](https://cloud.google.com/genomics/v1alpha2/pipelines#get_ready)
instructions.

- Authorize applications such as dsub to access Google Cloud resources by creating
[Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials). Run the command:

    [gcloud auth application-default login](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login)

- Clone this repository to your local workstation

- Change directory into the root directory of the local clone of the repository.

- Setup a virtualenv (optional, but very strongly recommended).

    ```bash
    virtualenv dsub_libs
    source dsub_libs/bin/activate
    ```

- Install dependent libraries

    ```bash
    pip install --upgrade oauth2client==1.5.2 google-api-python-client python-dateutil pytz tabulate
    ```

And then you can run with:

-   `./ddel [flags]`
-   `./dstat [flags]`
-   `./dsub [flags] my_script.sh`

## Help

To see the full set of parameters for dsub, dstat, ddel, pass the
`--help` flag.

## Examples

A simple getting started example can be found in the directory:

* examples/[decompress](examples/decompress)
