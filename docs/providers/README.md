# dsub backend providers

The "ideal" `dsub` user scenario is one in which you can develop and test
code on your local system (your own laptop or workstation) and then with just
a few changes to command-line parameters, run at scale in a Cloud environment
or on-premesis compute cluster.

To this end, `dsub` supports multiple "backend providers", each of which
implements a consistent runtime environment. The current supported providers
are:

- local
- google-batch (the default)

## Runtime environment

Each provider supports the following elements in your Docker container's
runtime environment:

### Environment variables point to your `--input` files

When you write your commands that run in your Docker container, you should
always access your input files through the environment variables that
are set for them.
You may observe that providers consistently place input files under
`/mnt/data/input`, but *there is no requirement that providers do so*.
Use the environment variables described in
[Input and Output File Handling](../input_output.md)
documentation.

### Environment variables point to where to write `--output` files

When you write your commands that run in your Docker container, you shoud
always write output files to the locations specified by the
environment variables that are set for them.
You may observe that providers consistently expect output files to be
written to `/mnt/data/output`, but *there is no requirement that providers do so*.
Use the environment variables described in
[Input and Output File Handling](../input_output.md)
documentation.

### Your script name is preserved and the directory is "world-writeable"

The script that you specify for the `--script` flag will be written to a
writeable directory within your Docker container, and the name of your
script will be preserved.

Directory writeability means that you can write to that directory if you need
to. To get the directory name, get it from argv[0]. For example:

In Bash:

```
readonly SCRIPT_DIR="$(dirname "$0")"
```

In Python:

```
import os
import sys
SCRIPT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))
```

The script name is preserved (and executable file permissions set) such that
your script can be directly executed. For example if your script name is
`my-script.py`, then so long as your Docker image supports direct execution
of `.py` files, your script will be able to run.

### TMPDIR will be set to reference a directory on your "data disk"

Historically, the Google Pipelines API (which the Google providers depend on)
put the Docker container's `/tmp` directory on the Compute
Engine VM's boot disk, rather than the data disk that is created for `dsub`.
To avoid your needing to separately size both the boot disk and the data disk,
the Google providers create a `tmp` directory and set the
`TMPDIR` environment variable (supported by many tools) to point to it.

This separation of boot vs. data disk does not hold for the `local` provider,
but the `local` provider still sets `TMPDIR`. `dsub` scripts that need
large temporary space should write to `${TMPDIR}` rather than `/tmp`

### An empty "working directory" is created

When your script runs in your Docker container, the current working directory
will be empty and writeable. You can get the working directory when your
script starts in many different ways, typically:

In Bash:

```
readonly WORKING_DIR="$(pwd)"
```

In Python:

```
import os
WORKING_DIR = os.getcwd()
```

## Provider details

Details of the runtime environments are provided here to aid in the
day-to-day use of and debugging of problems you may encounter.
The specifics of where temporary directories and files are created are subject
to change. Your script code should access elements of the runtime environment
through environment variables as described above.

### `local` provider

The `local` provider creates a runtime workspace on the Docker host
(your local workstation or laptop) to execute the following sequence of events:

1. Create a directory to store task metadata.
2. Create a data directory to mount into the task's Docker container.
3. Copy files from the local file system or
[Google Cloud Storage](https://cloud.google.com/storage/docs/) to the
task data directory.
4. Run a Docker container and execute your `--script` or `--command`.
5. Copy files from the data directory to the local file system or
Google Cloud Storage.

#### Orchestration

The `local` provider starts a task, it creates and executes a script called
`runner.sh` which orchestrates copying input files, running Docker, and
copying output files.

#### File copying

The copying of files is performed in the host environment, not inside the
Docker container. This means that for copying to/from Google Cloud Storage,
the host environment requires a copy of
[gsutil](https://cloud.google.com/storage/docs/gsutil) to be installed.

#### Container runtime environment

The `local` provider creates a workspace directory under:

- `${TMPDIR}/dsub-local/<job-id>/task`

If `dsub` is called with `--task` then the word `task` at the end of the path
is replaced by the task id, for example:

- `${TMPDIR}/dsub-local/<job-id>/0`
- `${TMPDIR}/dsub-local/<job-id>/1`
- `${TMPDIR}/dsub-local/<job-id>/2`

for a job with 3 tasks.

Note: `TMPDIR` is commonly set to `/tmp` by default on most Unix systems.
`TMPDIR` is set to a path under `/var/folders` on some versions of MacOS.

Each task folder contains a `data` folder that is mounted by Docker.
The data folder contains:

-   `input`: location of automatically localized `--input` and
    `--input-recursive` parameter values.
-   `output`: location for script to write automatically delocalized `--output`
    and `--output-recursive` parameter values.
-   `script`: location of your dsub `--script` or `--command` script.
-   `tmp`: temporary directory for your script. `TMPDIR` is set to this
    directory.
-   `workingdir`: the working directory set before your script runs.

#### Task state and logging

The `local` runner supports the following "operation status" values:

- RUNNING
- SUCCESS
- FAILURE
- CANCELED

Execution of the dsub task is orchestrated by a script, `runner.sh`,
which the `local` provider writes to the task directory.

During execution, `runner.sh` writes the following files to record task state:

-   `log.txt`: log generated by runner.sh, listing the high-level events (eg.
    "starting Docker").
-   `stdout.txt`: stdout from your Docker container.
-   `stderr.txt`: stderr from your Docker container.
-   `runner-log.txt`: stdout and stderr of `runner.sh`. Errors copying from/to
    GCS would show up here (ie. for localization/delocalization). Otherwise only
    useful for debugging `dsub` itself.
-   `meta.yaml`: metadata about the job (used by `dstat`) such as `job-id`,
    `job-name`, `task-id`, `envs`, `inputs`, and `outputs`.

#### Resource requirements

The `local` provider does not support resource-related flags such as
`--min-cpu`, `--min-ram`, `--boot-disk-size`, or `--disk-size`.

### `google-cls-v2` and `google-batch` providers

The `google-cls-v2` and `google-batch` providers share a significant amount of
their implementation. The `google-cls-v2` provider utilizes the Google Cloud Life Sciences
Piplines API [v2beta](https://cloud.google.com/life-sciences/docs/apis)
while the `google-batch` provider utilizes the Google Cloud
[Batch API](https://cloud.google.com/batch/docs/reference/rest)
to queue a request for the following sequence of events:

1. Create a Google Compute Engine
[Virtual Machine (VM) instance](https://cloud.google.com/compute/docs/instances/).
2. Create a Google Compute Engine
[Persistent Disk](https://cloud.google.com/compute/docs/disks/) and mount it
as a "data disk".
3. Localize files from
[Google Cloud Storage](https://cloud.google.com/storage/docs/) to the data disk.
4. Run execute your `--script` or `--command` in your Docker container.
5. Delocalize files from the data disk to Google Cloud Storage.
6. Destroy the VM

#### Orchestration

When the Pipelines
[run()](https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.pipelines/run)
API is called, it creates an
[operation](https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.operations).
The Pipelines API service will then create the VM and disk when
the your Cloud Project has sufficient
[Compute Engine quota](https://cloud.google.com/compute/quotas).

When the VM starts, it runs a Compute Engine
[startup script](https://cloud.google.com/compute/docs/startupscript)
to launch the Pipelines API "worker" which runs a set of on-VM services,
and orchestrates execution of *a sequence* of Docker containers.
After the containers have exited, the worker shuts off the VM.

Execution of `dsub` features is handled by a series of Docker containers on the
VM. The sequence of containers executed is:

1. `logging` (copy logs to GCS; *run in background*)
2. `prepare` (prepare data disk and save your script to the data disk)
3. `localization` (copy GCS objects to the data disk)
4. `user-command` (execute the user command)
5. `delocalization` (copy files from the data disk to GCS)
6. `final_logging` (copy logs to GCS; *always run*)

The `prepare` step does the following:

1. Create runtime directories (`script`, `tmp`, `workingdir`).
2. Write the user `--script` or `--command` to a file and make it executable.
3. Create the directories for `--input` and `--output` parameters.

#### Container runtime environment

The data disk path in the Docker containers is:

- `/mnt/data`

The `/mnt/data` folder contains:

-   `input`: location of localized `--input` and `--input-recursive` parameters.
-   `output`: location for your script to write files to be delocalized for
    `--output` and `--output-recursive` parameters.
-   `script`: location of the your dsub `--script` or `--command` script.
-   `tmp`: temporary directory for the your script. `TMPDIR` is set to this
    directory.
-   `workingdir`: the working directory set before the your script runs.

#### Task status

The Pipelines API supports operation status of:

- done: false
- done: true (with no error)
- done: true (with error)

`dsub` interprets the above to provide task statuses of:

- RUNNING (`done: false`)
- SUCCESS (`done: true` with no `error`)
- FAILURE (`done: true` with `error` code != 1)
- CANCELED (`done: true` with `error` code 1)

Note that for historical reasons, while an operation is queued for execution
its status is `RUNNING`.

#### Logging

The `google-batch` provider saves 3 log files to Cloud Storage, every 5 minutes
to the `--logging` location specified to `dsub`:

- `[prefix].log`: log generated by all containers running on the VM
- `[prefix]-stdout.log`: stdout from your Docker container
- `[prefix]-stderr.log`: stderr from your Docker container

Logging paths and the `[prefix]` are discussed further in [Logging](../logging.md).

#### Resource requirements

The `google-batch` providers support many resource-related
flags to configure the Compute Engine VMs that tasks run on, such as
`--machine-type` or `--min-cores` and `--min-ram`, as well as `--boot-disk-size`
and `--disk-size`. Additional provider-specific parameters are available
and documented below.

##### Disk allocation

The Docker container launched by the Pipelines API will use the host VM boot
disk for the system services needed to orchestrate the set of docker actions
defined by `dsub`.  All other directories set up by `dsub` will be on the
data disk, including the `TMPDIR` (as discussed above). In general it should
be unnecessary for end-users to ever change the `--boot-disk-size` and they
should only need to set the `--disk-size`. One known exception is when very
large Docker images are used, as such images need to be pulled to the boot disk.

#### Provider specific parameters

The following `dsub` parameters are specific to the `google-batch` providers:

* [Location resources](https://cloud.google.com/about/locations)

    - `--location`:
      - Specifies the Google Cloud region to which the pipeline request will be
        sent and where operation metadata will be stored. The associated dsub task
        may be executed in another region if the `--regions` or `--zones`
        arguments are specified. (default: us-central1)

    - `--project`:
      - Cloud project ID in which to run the job.
    - `--regions`:
      - List of Google Compute Engine regions. Only one of `--zones` and
        `--regions` may be specified.
    - `--zones`:
      - List of Google Compute Engine zones.

- [Network resources](https://cloud.google.com/vpc/docs/overview)
    - `--network`:
      - The Compute Engine VPC network name to attach the VM's network interface
        to. The value will be prefixed with `global/networks/` unless it contains
        a `/`, in which case it is assumed to be a fully specified network
        resource URL.
    - `--subnetwork`:
      - The name of the Compute Engine subnetwork to attach the instance to.
    - `--use-private-address`:
      - If set to true, do not attach a public IP address to the VM.
        (default: False)
    - `--block-external-network`:
      - If set to true, prevents the container for the user's script/command
        from accessing the external network. (default: False)

- Per-task compute resources
    - `--boot-disk-size`:
      - Size (in GB) of the boot disk. (default: 10)
    - `--cpu-platform`:
      - The CPU platform to request. Supported values can be found at
        [Specifying a minimum CPU](https://cloud.google.com/compute/docs/instances/specify-min-cpu-platform)
    - `--disk-type`:
      - The disk type to use for the data disk. Valid values are `pd-standard`,
        `pd-ssd` and `local-ssd`. (default: `pd-standard`)
    - `--docker-cache-images`:
      - The Compute Engine Disk Images to use as a Docker cache. At the moment,
        only a single image is supported. Image passed must be of the form
        "projects/{PROJECT_ID}/global/images/{IMAGE_NAME}".
        Instructions for creating a disk image can be found at
        [Create private images](https://cloud.google.com/compute/docs/images/create-delete-deprecate-private-images)
    - `--machine-type`:
      - Provider-specific machine type.
    - `--preemptible`:
      - If `--preemptible` is given without a number, enables preemptible VMs
        for all attempts for all tasks. If a number value N is used, enables
        preemptible VMs for up to N attempts for each task. Defaults to not
        using preemptible VMs.
    - `--timeout`:
      - The maximum amount of time to give the task to complete. This includes
        the time spent waiting for a worker to be allocated. Time can be listed
        using a number followed by a unit. Supported units are s (seconds),
        m (minutes), h (hours), d (days), w (weeks). Example: '7d' (7 days).
        (default: '7d')

- [Task credentials](https://cloud.google.com/docs/authentication)
    - `--credentials-file`:
      - Path to a local file with JSON credentials for a service account.
    - `--scopes`:
        - Space-separated scopes for Google Compute Engine instances. If
          unspecified, provider will use

          - https://www.googleapis.com/auth/bigquery,
          - https://www.googleapis.com/auth/compute,
          - https://www.googleapis.com/auth/devstorage.full_control,
          - https://www.googleapis.com/auth/genomics,
          - https://www.googleapis.com/auth/logging.write,
          - https://www.googleapis.com/auth/monitoring.write
    - `--service-account`:
      - Email address of the service account to be authorized on the Compute
        Engine VM for each job task. If not specified, the default Compute
        Engine service account for the project will be used.

- Monitoring, logging, and debugging
    - `--enable-stackdriver-monitoring`:
      - If set to true, enables Stackdriver monitoring on the VM.
        (default: False)
    - `--log-interval`:
      - The amount of time to sleep between copies of log files from the task to
        the logging path. Time can be listed using a number followed by a unit.
        Supported units are s (seconds), m (minutes), h (hours).
        Example: '5m' (5 minutes). (default: '1m')
    - `--ssh`:
      - If set to true, start an ssh container in the background to allow you to
        log in using SSH and debug in real time. (default: False)

- GPU resources
    - `--accelerator-type`:
        - The Compute Engine accelerator type. See
          https://cloud.google.com/compute/docs/gpus/ for supported GPU types.

          Only NVIDIA GPU accelerators are currently supported. If an NVIDIA GPU
          is attached, the required runtime libraries will be made available to
          all containers under /usr/local/nvidia.

          Each version of Container-Optimized OS image (used by the Pipelines
          API) has a default supported NVIDIA GPU driver version. See
          https://cloud.google.com/container-optimized-os/docs/how-to/run-gpus#install

          Note that attaching a GPU increases the worker VM startup time by a
          few minutes. (default: None)
    - `--accelerator-count`:
      - The number of accelerators of the specified type to attach. By
        specifying this parameter, you will download and install the following
        third-party software onto your job's Compute Engine instances: NVIDIA(R)
        Tesla(R) drivers and NVIDIA(R) CUDA toolkit. (default: 0)
