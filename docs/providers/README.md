# dsub backend providers

The "ideal" `dsub` user scenario is one in which you can develop and test
code on your local system (your own laptop or workstation) and then with just
a few changes to command-line parameters, run at scale in a Cloud environment
or on-premesis compute cluster.

To this end, `dsub` supports multiple "backend providers", each of which
implements a consistent runtime environment. The current supported providers
are:

- local
- google (deprecated: use `google-v2`)
- google-v2 (the default)

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

The `google` and `google-v2` providers depend on the
[Google Genomics Pipelines API](https://cloud.google.com/genomics/reference/rest/v1alpha2/pipelines)
and
[Google Genomics Pipelines API](https://cloud.google.com/genomics/reference/rest/v2alpha1/pipelines)
respectively, which put the Docker container's `/tmp` directory on the Compute
Engine VM's boot disk, rather than the data disk that is created for `dsub`.
To avoid your needing to separately size both the boot disk and the data disk,
the `google` and `google-v2` providers create a `tmp` directory and set the
`TMPDIR` environment variable (supported by many tools) to point to it.

The separation of boot vs. data disk does not hold for the `local` provider,
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

- `/tmp/dsub-local/<job-id>/task`

If `dsub` is called with `--task` then the word `task` at the end of the path
is replaced by the task id, for example:

- `/tmp/dsub-local/<job-id>/0`
- `/tmp/dsub-local/<job-id>/1`
- `/tmp/dsub-local/<job-id>/2`

for a job with 3 tasks.

Each task folder contains a `data` folder that is mounted by Docker.
The data folder contains:

-   `input`: location of automatically localized `--input` and
    `--input-recursive` parameter values.
-   `output`: location for script to write automatically delocalized `--output`
    and `--output-recursive` parameter values.
-   `script`: location of the your dsub `--script` or `--command` script.
-   `tmp`: temporary directory for the your script. `TMPDIR` is set to this
    directory.
-   `workingdir`: the working directory set before the your script runs.

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

### `google` provider

The `google` provider utilizes the Google Genomics Pipelines API
[pipelines.run()](https://cloud.google.com/genomics/reference/rest/v1alpha2/pipelines/run)
to queue a request for the following sequence of events:

1. Create a Google Compute Engine
[Virtual Machine (VM) instance](https://cloud.google.com/compute/docs/instances/).
2. Create a Google Compute Engine
[Persistent Disk](https://cloud.google.com/compute/docs/disks/) and mount it
as a "data disk".
3. Localize files from
[Google Cloud Storage](https://cloud.google.com/storage/docs/) to the data disk.
4. Run a Docker container and execute your `--script` or `--command`.
5. Delocalize files from the data disk to Google Cloud Storage.
6. Destroy the VM

#### Orchestration

When the pipelines.run() API is called, it creates an
[operation](https://cloud.google.com/genomics/reference/rest/v1alpha2/operations).
The Pipelines API service will then create the VM and disk when
the your Cloud Project has sufficient
[Compute Engine quota](https://cloud.google.com/compute/quotas).

When the VM starts, it runs a Compute Engine
[startup script](https://cloud.google.com/compute/docs/startupscript)
to launch the Pipelines API "controller" which orchestrates input localization,
Docker execution, and output delocalization.

When the controller exits, the startup script destroys the VM and disk.

While the Pipelines API provides the core infrastructure for job queuing,
VM creation, and Docker command execution, several features of `dsub` are
not directly supported by the Pipelines API. These features are implemented
by wrapping the user `--script` or `--command` with code that runs inside the
Docker container.

The Docker command submitted to the Pipelines API for a `dsub` does the
following:

1. Create runtime directories (`script`, `tmp`, `workingdir`) described above.
2. Write the user `--script` or `--command` to a file and make it executable.
3. Install `gsutil` if there are recursive copies to do.
4. Set environment variables for `--input` parameters with wildcards.
5. Set environment variables for `--input-recursive` parameters.
6. Perform copy for `--input-recursive` parameters.
7. Create the directories for `--output` parameters.
8. Set environment variables for `--output-recursive` parameters.
9. Set `TMPDIR`.
10. Set the working directory.
11. **Run the user `--script` or `--command`**.
12. Perform copy for `--output-recursive` parameters.

#### File copying

Note that for the `--input-recursive` and `--output-recursive` features,
`dsub` does not get direct support from the Pipelines API. Instead, it injects
the necessary code into the Docker command to set the environment variables and
perform the recursive copies.

As a getting started convenience, if `--input-recursive` or `--output-recursive`
are used with the `google` provider, `dsub` will automatically check for and
install the
[Google Cloud SDK](https://cloud.google.com/sdk/docs/) in the Docker container
at runtime (before your script executes).

If you use the recursive copy features, install the Cloud SDK in your Docker
image when you build it to avoid the installation at runtime.

#### Container runtime environment

The data disk path is the same on the host VM as it is in the Docker container:

- `/mnt/data`

The `/mnt/data` folder contains:

-   `input`: location of automatically localized `--input` and
    `--input-recursive` parameter values.
-   `output`: location for script to write automatically delocalized `--output`
    and `--output-recursive` parameter values.
-   `script`: location of the your dsub `--script` or `--command` script.
-   `tmp`: temporary directory for the your script. `TMPDIR` is set to this
    directory.
-   `workingdir`: the working directory set before the your script runs.

#### Task state and logging

The Genomics API supports the following "operation status" values:

- RUNNING
- SUCCESS
- FAILURE
- CANCELED

Note that while an operation is queued for execution its status is `RUNNING`.

The Pipelines API controller maintains 3 log files, which are uploaded
every 5 minutes to the `--logging` location specified to `dsub`:

- `[prefix].log`: log generated by the controller as it executes
- `[prefix]-stdout.log`: stdout from your Docker container
- `[prefix]-stderr.log`: stderr from your Docker container

See [Logging](https://github.com/DataBiosphere/dsub/blob/master/docs/logging.md)
for more details on log files.

#### Resource requirements

The `google` provider supports resource-related flags such as
`--min-cpu`, `--min-ram`, `--boot-disk-size`, or `--disk-size`.

##### Machine type

By default, the Compute Engine VM that runs will be an `n1-standard-1`.
If you specify `--min-cpu` and/or `--min-ram`, the Pipelines API will
choose the smallest
[predefined machine type](https://cloud.google.com/compute/docs/machine-types)
that satisfies your requested minimums.

##### Disk allocation

The Docker container launched by the Pipelines API will use the host VM boot
disk for system paths. All other directories set up by `dsub` will be on the
data disk, including the `TMPDIR` (as discussed above). Thus you should only
ever need to change the `--disk-size`.

### `google-v2` provider

The `google-v2` provider utilizes the Google Genomics Pipelines API
[pipelines.run()](https://cloud.google.com/genomics/reference/rest/v2alpha1/pipelines/run)
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

When the pipelines.run() API is called, it creates an
[operation](https://cloud.google.com/genomics/reference/rest/v2alpha1/operations).
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

The Genomics `v2alpha1` API supports operation status of:

- done: false
- done: true with no error
- done: true with error

`dsub` interprets the above to provide task statuses of:

- RUNNING
- SUCCESS
- FAILURE
- CANCELED

Note that for historical reasons, while an operation is queued for execution
its status is `RUNNING`.

#### Logging

The `google-v2` provider saves 3 log files to Cloud Storage, every 5 minutes
to the `--logging` location specified to `dsub`:

- `[prefix].log`: log generated by all containers running on the VM
- `[prefix]-stdout.log`: stdout from your Docker container
- `[prefix]-stderr.log`: stderr from your Docker container

Logging paths and the `[prefix]` are discussed further in [Logging](../logging.md).

#### Resource requirements

The `google-v2` provider supports resource-related flags such as
`--machine-type`, `--boot-disk-size`, `--disk-size`, and several other
Compute Engine VM parameters.

##### Disk allocation

The Docker container launched by the Pipelines API will use the host VM boot
disk for system paths. All other directories set up by `dsub` will be on the
data disk, including the `TMPDIR` (as discussed above). Thus you should only
ever need to change the `--disk-size`.
