# Job control

dsub provides two simple mechanisms for specifying job dependencies. This
enables you to sequence or run in parallel multiple jobs, each of which can have
its own resource requirements, such as CPU and memory.

## Example with --wait

If your job dependencies are simple - one job needs to run after another - you
can simply use the "--wait" flag for each job that you launch.

The "--wait" flag will cause dsub to block after launching your job. It will
continue to poll until your job completes. If the job completes successfully,
then dsub will exit with status code 0, otherwise dsub exits with status code 1.

You can take advantage of the "errexit" option in bash:

```
# Enable exit on error
set -o errexit

# Launch step 1 and block until completion
dsub ... --wait

# Launch step 2 and block until completion
dsub ... --wait

# Launch step 3
dsub ...

If you want to handle the error explicitly, the dsub exit status can be handled as:

# Launch step 1 and block until completion
if ! dsub ... --wait; then
  echo "Step 1 failed!"
  exit 1
fi

## Example with --after

Suppose you want to run jobs A and B in parallel, and then job C.

This can be done with `--after`, passing it the job-id from A and B.

The first step is to capture the job-id for the job you want to wait for.

```
JOBID_A=$(./dsub ...)
JOBID_B=$(./dsub ...)
```

Then pass these value to the `--after` flag for launching Job C

```
./dsub ... --after "${JOB_A}" "${JOB_B}"
```

Here is the output of a sample run:

```
$ JOBID_A=$(./dsub --project "${MYPROJECT}" --zones "us-central1-*" \
--logging "gs://${MYBUCKET}/logging/"   \
--command 'echo "hello from job A"')
Job: echo--<userid>--170328-093205-80
Launched job-id: echo--<userid>--170328-093205-80
To check the status, run:
  dstat --project test-project --jobs echo--<userid>--170328-093205-80
To cancel the job, run:
  ddel --project test-project --jobs echo--<userid>--170328-093205-80

$ echo "${JOBID_A}"
echo--<userid>--170328-093205-80

$ JOBID_B=... (similar)

$ JOBID_C=$(./dsub --project "${MYPROJECT}" --zones "us-central1-*" \
--logging "gs://${MYBUCKET}/logging/"   \
--command 'echo "job C"' --after "${JOBID_A}" "${JOBID_B}")
Job: echo--<userid>--170328-093415-23
Waiting for predecessor jobs to complete...
Waiting for: echo--<userid>--170328-093358-55, echo--<userid>--170328-093205-80.
  echo--<userid>--170328-093205-80: ('Success', '2017-03-28 09:32:58')
Waiting for: echo--<userid>--170328-093358-55.
  echo--<userid>--170328-093358-55: ('Success', '2017-03-28 09:35:00')
Launched job-id: echo--<userid>--170328-093415-23
To check the status, run:
  dstat --project test-project --jobs echo--<userid>--170328-093415-23
To cancel the job, run:
  ddel --project test-project --jobs echo--<userid>--170328-093415-23
```

## --after is blocking

The `--after` command works by blocking dsub until the previous job(s) complete. This means
that the submission will fail if you turn off your computer before that happens.

If you'd like to be able to turn off your machine (perhaps it's a laptop and you're running
a big job) then what you can do is put your dsub command itself into a script,
and run that script using dsub itself.

