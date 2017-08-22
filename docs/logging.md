# Logging

Both the `local` and `google` providers emit logging in a similar fashion:

-   Three log files are written to disk in the local execution environment.
    -   Task log
    -   Docker command stdout
    -   Docker command stderr
-   The log files are copied from the execution environment to a designated
    logging path on completion of the task. Some providers will update the log
    files periodically during task execution.

## Logging paths

Logging paths can take two forms:

-   A directory path
-   A path + file ending in ".log"

Each of these path forms support insertion of dsub job-related information (such
as the `job-id`) into the path.

### Providers

The `local` provider supports both `file://` as well as `gs://` logging paths.

The `google` provider supports `gs://` logging paths.

### Directory path

When a path not ending ".log" is provided, it is treated like a directory. For
example if you specify `gs://my-bucket/my-path/my-pipeline`, the log file names
generated will be:

-   `gs://my-bucket/my-path/my-pipeline/{job-id}.log`
-   `gs://my-bucket/my-path/my-pipeline/{job-id}-stderr.log`
-   `gs://my-bucket/my-path/my-pipeline/{job-id}-stdout.log`

or for `--tasks` jobs:

-   `gs://my-bucket/my-path/my-pipeline/{job-id}.{task-id}.log`
-   `gs://my-bucket/my-path/my-pipeline/{job-id}.{task-id}-stderr.log`
-   `gs://my-bucket/my-path/my-pipeline/{job-id}.{task-id}-stdout.log`

### A path + file ending in ".log"

You can also specify a path that ends in ".log". For example if you specify:
`gs://my-bucket/my-path/my-pipeline.log`, then the log file names generated will
be:

-   `gs://my-bucket/my-path/my-pipeline.log`
-   `gs://my-bucket/my-path/my-pipeline-stderr.log`
-   `gs://my-bucket/my-path/my-pipeline-stdout.log`

or for `--tasks` jobs:

-   `gs://my-bucket/my-path/my-pipeline.{task-id}.log`
-   `gs://my-bucket/my-path/my-pipeline.{task-id}-stderr.log`
-   `gs://my-bucket/my-path/my-pipeline.{task-id}-stdout.log`

### Inserting job data

You may want to structure your logging output differently than either of the
above allow. For example, you may want output of the form:

-   `gs://my-bucket/my-path/my-pipeline/{job-name}/{task-id}/task.log`
-   `gs://my-bucket/my-path/my-pipeline/{job-name}/{task-id}/task-stderr.log`
-   `gs://my-bucket/my-path/my-pipeline/{job-name}/{task-id}/task-stdout.log`

dsub supports logging paths that include format strings, with substitution
variables enclosed in curly braces: `{var}`.

Supported variables are:

-   `job-id`
-   `job-name`
-   `task-id`
-   `user-id`

