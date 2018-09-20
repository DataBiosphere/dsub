# dsub: simple batch jobs with Docker
[![License](https://img.shields.io/badge/license-Apache%202.0-brightgreen.svg)](https://github.com/DataBiosphere/dsub/blob/master/LICENSE)

## Overview

`dsub` is a command-line tool that makes it easy to submit and run batch scripts
in the cloud.

The `dsub` user experience is modeled after traditional high-performance
computing job schedulers like Grid Engine and Slurm. You write a script and
then submit it to a job scheduler from a shell prompt on your local machine.

Today `dsub` supports Google Cloud as the backend batch job runner, along with a
local provider for development and testing. With help from the community, we'd
like to add other backends, such as a Grid Engine, Slurm, Amazon Batch,
and Azure Batch.

## Getting started

You can install `dsub` from [PyPI](pypi.python.org), or you can clone and
install from this github repository.

Note: `dsub` was written for Python 2.7 and production users of `dsub`
should continue using Python 2.7. As of `dsub` v0.2.0, we have enabled
experimental support of Python 3.5+.


### Pre-installation steps

1. This is optional, but whether installing from PyPI or from github,
you are encouraged to use a [Python virtualenv](https://virtualenv.pypa.io).

    If necessary, [install virtualenv](https://virtualenv.pypa.io/en/stable/installation/).

1.  Create and activate a Python virtualenv.

        # (You can do this in a directory of your choosing.)
        virtualenv --python=python2.7 dsub_libs
        source dsub_libs/bin/activate

### Install `dsub`

Choose one of the following:

#### Install from PyPI

1.  If necessary, [install pip](https://pip.pypa.io/en/stable/installing/).

1.  Install `dsub`

         pip install dsub

#### Install from github

1.  Be sure you have git installed

    Instructions for your environment can be found on the
    [git website](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

1.  Clone this repository.

        git clone https://github.com/DataBiosphere/dsub
        cd dsub

1.  Install dsub (this will also install the dependencies)

        python setup.py install

1.  Set up Bash tab completion (optional).

        source bash_tab_complete

### Post-installation steps

1.  Minimally verify the installation by running:

        dsub --help

1.  (Optional) [Install Docker](https://docs.docker.com/engine/installation/).

    This is necessary only if you're going to create your own Docker images or
    use the `local` provider.

### Getting started with the local provider

We think you'll find the `local` provider to be very helpful when building
your pipelines. You'll get quicker turnaround times and won't incur cloud
charges using it.

1. Run a `dsub` job and wait for completion.

    Here is a very simple "Hello World" test:

        dsub \
          --provider local \
          --logging /tmp/dsub-test/logging/ \
          --output OUT=/tmp/dsub-test/output/out.txt \
          --command 'echo "Hello World" > "${OUT}"' \
          --wait

1. View the output file.

        cat /tmp/dsub-test/output/out.txt

### Getting started on Google Cloud

1.  Sign up for a Google Cloud Platform account and
    [create a project](https://console.cloud.google.com/project?).

1.  [Enable the APIs](https://console.cloud.google.com/flows/enableapi?apiid=genomics,storage_component,compute_component&redirect=https://console.cloud.google.com).

1.  [Install the Google Cloud SDK](https://cloud.google.com/sdk/) and run

        gcloud init

    This will set up your default project and grant credentials to the Google
    Cloud SDK. Now provide [credentials](https://developers.google.com/identity/protocols/application-default-credentials)
    so dsub can call Google APIs:

        gcloud auth application-default login

1.  Create a [Google Cloud Storage](https://cloud.google.com/storage) bucket.

    The dsub logs and output files will be written to a bucket. Create a
    bucket using the [storage browser](https://cloud.google.com/storage/browser?project=)
    or run the command-line utility [gsutil](https://cloud.google.com/storage/docs/gsutil), included in
    the Cloud SDK.

        gsutil mb gs://my-bucket

    Change `my-bucket` to a unique name that follows the
    [bucket-naming conventions](https://cloud.google.com/storage/docs/bucket-naming).

    (By default, the bucket will be in the US, but you can change or
    refine the [location](https://cloud.google.com/storage/docs/bucket-locations) setting with the
    `-l` option.)

1.  Run a dsub job and wait for completion.

    Here is a very simple "Hello World" test:

        dsub \
          --project my-cloud-project \
          --zones "us-central1-*" \
          --logging gs://my-bucket/logging/ \
          --output OUT=gs://my-bucket/output/out.txt \
          --command 'echo "Hello World" > "${OUT}"' \
          --wait

    Change `my-cloud-project` to your Google Cloud project, and `my-bucket` to
    the bucket you created above.

    The output of the script command will be written to the `OUT` file in Cloud
    Storage that you specify.

1. View the output file.

        gsutil cat gs://my-bucket/output/out.txt


### Getting started with the `google-v2` provider

Google Cloud has made available a new version of the Google Genomics
Pipelines API. This version, `v2alpha1`, will soon replace the `v1alpha2`
verision that dsub's `google` provider uses.

To use the `google-v2` provider:

- Add `--provider google-v2` to your command-line
- Use `--machine-type` (default is `n1-standard-1`).

The `--machine-type` value can be one of the
[Predefined Machine Types](https://cloud.google.com/compute/docs/machine-types#predefined_machine_types)
or a
[Custom Machine Type](https://cloud.google.com/compute/docs/machine-types#custom_machine_types).

The `google` provider supports `--min-cpu` and `--min-ram`. A plan to support
these flags for `google-v2` is being evaluated.
See [google-v2 support](https://github.com/DataBiosphere/dsub/issues/114).

## `dsub` features

The following sections show how to run more complex jobs.

### Defining what code to run

You can provide a shell command directly in the dsub command-line, as in the
hello example above.

You can also save your script to a file, like `hello.sh`. Then you can run:

    dsub \
        --project my-cloud-project \
        --zones "us-central1-*" \
        --logging gs://my-bucket/logging \
        --script hello.sh

If your script has dependencies that are not stored in your Docker image,
you can transfer them to the local disk. See the instructions below for
working with input and output files and folders.

### Selecting a Docker image

By default, dsub uses a stock Ubuntu image. You can change the image
by passing the `--image` flag.

    dsub \
        --project my-cloud-project \
        --zones "us-central1-*" \
        --logging gs://my-bucket/logging \
        --image ubuntu:16.04 \
        --script hello.sh

### Passing parameters to your script

You can pass environment variables to your script using the `--env` flag.

    dsub \
        --project my-cloud-project \
        --zones "us-central1-*" \
        --logging gs://my-bucket/logging \
        --env MESSAGE=hello \
        --command 'echo ${MESSAGE}'

The environment variable `MESSAGE` will be assigned the value `hello` when
your Docker container runs.

Your script or command can reference the variable like any other Linux
environment variable, as `${MESSAGE}`.

**Be sure to enclose your command string in single quotes and not double
quotes. If you use double quotes, the command will be expanded in your local
shell before being passed to dsub. For more information on using the
`--command` flag, see [Scripts, Commands, and Docker](docs/code.md)**

To set multiple environment variables, you can repeat the flag:

    --env VAR1=value1 \
    --env VAR2=value2

You can also set multiple variables, space-delimited, with a single flag:

    --env VAR1=value1 VAR2=value2

### Working with input and output files and folders

dsub mimics the behavior of a shared file system using cloud storage
bucket paths for input and output files and folders. You specify
the cloud storage bucket path. Paths can be:

* file paths like `gs://my-bucket/my-file`
* folder paths like `gs://my-bucket/my-folder`
* wildcard paths like `gs://my-bucket/my-folder/*`

See the [inputs and outputs](docs/input_output.md) documentation for more details.

### Transferring input files to a Google Cloud Storage bucket.

If your script expects to read local input files that are not already
contained within your Docker image, the files must be available in Google
Cloud Storage.

If your script has dependent files, you can make them available to your script
by:

 * Building a private Docker image with the dependent files and publishing the
   image to a public site, or privately to Google Container Registry
 * Uploading the files to Google Cloud Storage

To upload the files to Google Cloud Storage, you can use the
[storage browser](https://console.cloud.google.com/storage/browser?project=) or
[gsutil](https://cloud.google.com/storage/docs/gsutil). You can also run on data
that’s public or shared with your service account, an email address that you
can find in the [Google Cloud Console](https://console.cloud.google.com).

#### Files

To specify input and output files, use the `--input` and `--output` flags:

    dsub \
        --project my-cloud-project \
        --zones "us-central1-*" \
        --logging gs://my-bucket/logging \
        --input INPUT_FILE=gs://my-bucket/my-input-file \
        --output OUTPUT_FILE=gs://my-bucket/my-output-file \
        --command 'cat ${INPUT_FILE} > ${OUTPUT_FILE}'

The input file will be copied from `gs://my-bucket/my-input-file` to a local
path given by the environment variable `${INPUT_FILE}`. Inside your script, you
can reference the local file path using the environment variable.

The output file will be written to local disk at the location given by
`${OUTPUT_FILE}`. Inside your script, you can reference the local file path
using the environment variable. After the script completes, the output file
will be copied to the bucket path `gs://my-bucket/my-output-file`.

#### Folders

To copy folders rather than files, use the `--input-recursive` or
`output-recursive` flags:

    dsub \
        --project my-cloud-project \
        --zones "us-central1-*" \
        --logging gs://my-bucket/logging \
        --input-recursive FOLDER=gs://my-bucket/my-folder \
        --command 'find ${FOLDER} -name "foo*"'

##### Notice

As a getting started convenience, if `--input-recursive` or `--output-recursive`
are used, `dsub` will automatically check for and, if needed, install the
[Google Cloud SDK](https://cloud.google.com/sdk/docs/) in the Docker container
at runtime (before your script executes).

If you use the recursive copy features, install the Cloud SDK in your Docker
image when you build it to avoid the installation at runtime.

If you use a Debian or Ubuntu Docker image, you are encouraged to use the
[package installation instructions](https://cloud.google.com/sdk/downloads#apt-get).

If you use a Red Hat or CentOS Docker image, you are encouraged to use the
[package installation instructions](https://cloud.google.com/sdk/downloads#yum).

### Setting resource requirements

`dsub` tasks run using the `local` provider will use the resources available on
your local machine.

`dsub` tasks run using the `google` or `google-v2` providers can take advantage
of a wide range of CPU, RAM, disk, and hardware accelerator (eg. GPU) options.

See the [Compute Resources](docs/compute_resources.md) documentation for
details.

### Submitting a batch job

Each of the examples above has demonstrated submitting a single task with
a single set of variables, inputs, and outputs. If you have a batch of inputs
and you want to run the same operation over them, `dsub` allows you
to create a batch job.

Instead of calling `dsub` repeatedly, you can create
a tab-separated values (TSV) file containing the variables,
inputs, and outputs for each task, and then call `dsub` once.
The result will be a single `job-id` with multiple tasks. The tasks will
be scheduled and run independently, but can be
[monitored](#viewing-job-status) and [deleted](#deleting-a-job) as a group.

#### Tasks file format

The first line of the TSV file specifies the names and types of the
parameters. For example:

    --env SAMPLE_ID<tab>--input VCF_FILE<tab>--output OUTPUT_PATH

Each addition line in the file should provide the variable, input, and output
values for each task. Each line beyond the header represents the values for a
separate task.

Multiple `--env`, `--input`, and `--output` parameters can be specified and
they can be specified in any order. For example:

    --env SAMPLE<tab>--input A<tab>--input B<tab>--env REFNAME<tab>--output O
    S1<tab>gs://path/A1.txt<tab>gs://path/B1.txt<tab>R1<tab>gs://path/O1.txt
    S2<tab>gs://path/A2.txt<tab>gs://path/B2.txt<tab>R2<tab>gs://path/O2.txt


#### Tasks parameter

Pass the TSV file to dsub using the `--tasks` parameter. This parameter
accepts both the file path and optionally a range of tasks to process.
The file may be read from the local filesystem (on the machine you're calling
`dsub` from), or from a bucket in Google Cloud Storage (file name starts with
"gs://").

For example, suppose `my-tasks.tsv` contains 101 lines: a one-line header and
100 lines of parameters for tasks to run. Then:

```
dsub ... --tasks ./my-tasks.tsv
```

will create a job with 100 tasks, while:

```
dsub ... --tasks ./my-tasks.tsv 1-10
```

will create a job with 10 tasks, one for each of lines 2 through 11.

The task range values can take any of the following forms:

*   `m` indicates to submit task `m` (line m+1)
*   `m-` indicates to submit all tasks starting with task `m`
*   `m-n` indicates to submit all tasks from `m` to `n` (inclusive).

### Logging

The `--logging` flag points to a location for `dsub` task log files. For details
on how to specify your logging path, see [Logging](docs/logging.md).

### Job control

It's possible to wait for a job to complete before starting another.
For details, see [job control with dsub](docs/job_control.md).

### Retries

It is possible for `dsub` to automatically retry failed tasks.
For details, see [retries with dsub](docs/retries.md).

### Labeling jobs and tasks

You can add custom labels to jobs and tasks, which allows you to monitor and
cancel tasks using your own identifiers. In addition, with the `google`
provider, labeling a task will label associated compute resources such as
virtual machines and disks.

For more details, see [Checking Status and Troubleshooting Jobs](docs/troubleshooting.md)

### Viewing job status

The `dstat` command displays the status of jobs:

    dstat --project my-cloud-project

With no additional arguments, dstat will display a list of *running* jobs for
the current `USER`.

To display the status of a specific job, use the `--jobs` flag:

    dstat --project my-cloud-project --jobs job-id

For a batch job, the output will list all *running* tasks.

Each job submitted by dsub is given a set of metadata values that can be
used for job identification and job control. The metadata associated with
each job includes:

*   `job-name`: defaults to the name of your script file or the first word of
    your script command; it can be explicitly set with the `--name` parameter.
*   `user-id`: the `USER` environment variable value.
*   `job-id`: takes the form `job-name--userid--timestamp` where the `job-name`
    is truncated at 10 characters and the `timestamp` is of the form
    `YYMMDD-HHMMSS-XX`, unique to hundredths of a second.
*   `task-id`: if the job is submitted with the `--tasks` parameter, each task
    gets a sequential value of the form "task-*n*" where *n* is 1-based.

Metadata can be used to cancel a job or individual tasks within a batch job.

#### Summarizing job status

By default, dstat outputs one line per task. If you're using a batch job with
many tasks then you may benefit from `--summary`.

```
$ dstat --project my-project --summary

Job Name        Status         Task Count
-------------   -------------  -------------
my-job-name     RUNNING        2
my-job-name     SUCCESS        1
```

In this mode, dstat prints one line per (job name, task status) pair. You can
see at a glance how many tasks are finished, how many are still running, and
how many are failed/canceled.

### Deleting a job

The `ddel` command will delete running jobs.

By default, only jobs submitted by the current user will be deleted.
Use the `--users` flag to specify other users, or `"*"` for all users.

To delete a running job:

    ddel --project my-cloud-project --jobs job-id

If the job is a batch job, all running tasks will be deleted.

To delete specific tasks:

    ddel \
        --project my-cloud-project \
        --jobs job-id \
        --tasks task-id1 task-id2

To delete all running jobs for the current user:

    ddel --project my-cloud-project --jobs "*"

## What next?

*   See the examples:

    *   [Custom scripts](examples/custom_scripts)
    *   [Decompress files](examples/decompress)
    *   [FastQC](examples/fastqc)
    *   [Samtools index](examples/samtools)

*   See more documentation for:

    *   [Scripts, Commands, and Docker](docs/code.md)
    *   [Input and Output File Handling](docs/input_output.md)
    *   [Logging](docs/logging.md)
    *   [Compute Resources](docs/compute_resources.md)
    *   [Job Control](docs/job_control.md)
    *   [Retries](docs/retries.md)
    *   [Checking Status and Troubleshooting Jobs](docs/troubleshooting.md)
    *   [Backend providers](docs/providers/README.md)
