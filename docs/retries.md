# Retries

When running `dsub` jobs, there are times when tasks fail.

Sometimes the task is failing for a permanent reason such as insufficient disk
space or a bad path to an input file in Cloud Storage.

Other times, the task has failed simply because a network resource (a Docker
image or an input file) was temporarily unavailable.

In the case of temporary failures it would be very helpful if the task were
automatically retried.

## Using dsub retries

Automatic retries are implemented in `dsub` in conjunction with the `--wait`
option. The `dsub` process which has launched your job will poll for status
of each of the job tasks and will retry those that are retryable.

The `dsub` process will exit when no task is still running and no tasks are
retryable. The `dsub` process will exit with an exit code of 0 if the final
attempt of all tasks completed with status SUCCESS.

### Retryable tasks

A task is considered to be retryable if:

- the latest attempt has FAILED
  - the latest attempt has completed
  - the latest attempt has not succeeded (status SUCCESS)
  - the latest attempt was not canceled (status CANCELED)
- the task has not been retried the maximum number of times

### --retries flag

To specify the maximum number of per-task retries, use the `--retries` flag.
For example `--retries 3` will retry each task 3 times before failing.

## Tracking task attempts

When viewing tasks with `dstat --full` the attempt number will be available
as `task-attempt`.

## Logging

Logs for the retry will overwrite the original logs for that task.

## Limitations

### Early termination of `dsub`

Retries will only occur while `dsub` is active. If `dsub` exits abnormally,
tasks that have already been submitted will continue to run but additional
failures will not be retried.

A recommended way to recover from this is to wait for all currently running
tasks to complete, and then re-run `dsub` using the `--skip` flag. This will
instruct `dsub` to skip running tasks that have already written their output.

See [job control with dsub](job_control.md) for more information on the
`--skip` flag.

### Concurrent `dsub` execution

A job that is started with `--retries` should not be used with another job's
`--after` because `--after` will not wait for the predecessor job's tasks to
be retried.

## Future work

We would like to make `dsub` retries even more robust. Some notable future
features:

- Enable `dstat` to monitor a job and initiate retries
- Enable retries with changes to the runtime environment:
  - more disk
  - more memory
  - switch from preemptible to non-preemptible VMs
- Add a maximum total number of retries for all tasks for a job
