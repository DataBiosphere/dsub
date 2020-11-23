# Checking Status and Troubleshooting Jobs

Once jobs are launched using `dsub`, you'll typically want to check on status.
If something goes wrong, you'll want to discover why.

## Checking status

`dstat` allows you to get status information for your jobs and supports
filtering of output based on 3 fields. Each field can take a list of values
to filter on:

* `job-id`: one or more job-id values
* `user-id`: the `$USER` who submitted the job, or `'*'` for all users
* `status`: `RUNNING`, `SUCCESS`, `FAILURE`, `CANCELED`, or `'*'` for all job
statuses

### Check all my running jobs

When submitted with no filter arguments, `dstat` shows  information for all
tasks in the `RUNNING` state belonging to the current user:

```
$ dstat --provider google-v2 --project my-project
Job Name        Task    Status            Last Update
--------------  ------  ----------------  -------------------
my-job-name     task-3  localizing-files  2017-04-06 16:03:34
my-job-name     task-2  localizing-files  2017-04-06 16:03:33
my-job-name     task-1  Pending           2017-04-06 16:02:39
```

The above output is for a single job which includes 3 individual tasks
specified in a TSV file. The job id is omitted from the default output
for brevity. To see the job id, use `--full` flag as is described in
[Getting detailed job information](#getting-detailed-job-information).

To group together all the tasks from the same job with the same status,
use the `--summary` flag as described in the
[README](../README.md#summarizing-job-status).

### Check one of my jobs

If you are running multiple jobs concurrently, you may want to check status on
them separately. To check on a specific job, pass the `--jobs` (or `-j`)
flag. For example:

```
$ dstat --provider google-v2 --project my-project --jobs my-job-id
Job Name        Status    Last Update
--------------  --------  -------------------
my-job-name     Pending   2017-04-11 16:05:35
```

### Check one of my completed jobs

If you find that `dstat` produces no output for a particular job, it means that
the job completed (SUCCESS, FAILURE, or CANCELED).
To check a specific job independent of status, pass the
value `*` to `dstat`:

```
$ dstat \
  --provider google-v2 \
  --project my-project \
  --jobs my-job-id \
  --status '*'
Job Name        Status                          Last Update
--------------  ------------------------------  -------------------
my-job-name     Operation canceled at 2017-...  2017-04-11 16:07:02
```

Be sure to quote the `*` to prevent shell expansion.

### Check all of my jobs

To view results for all jobs associated with your user id:

```
dstat --provider google-v2 --project my-project --status '*'
```

### Check jobs with my own labels

Jobs and tasks can have arbitrary labels attached at launch time.
These labels can then be used by `dstat` and `ddel` for lookup.

#### Setting labels with `dsub`

You can set labels on your job using the `--label` flag. For example:

```
dsub \
  --label 'billing-code=c9' \
  --label 'batch=august-2017' \
  ...
```

You can set labels in your `--tasks` file. For example:

```
--label billing-code<tab>--label batch<tab>--label sample-id<tab>--env ...
a9<tab>august-2017<tab>sam001<tab>...
h25<tab>august-2017<tab>sam002<tab>...
```

#### Looking up by labels with `dstat`

To look up jobs by label with `dstat`, specify one or more `--label` flags
on the command line. Lookups match *all* labels specified (a logical `AND`).

For example, looking up all tasks from the above `--tasks` example:

```
dstat \
  --label 'billing-code=a9' \
  --label 'batch=august-2017' \
  --status '*' \
  ...
```

Will match all jobs with the `billing-code` label of `a9`, while:

```
dstat \
  --label 'billing-code=999' \
  --label 'batch=august-2017' \
  --label 'sample-id=sam002' \
  --status '*' \
  ...
```

will match only the second task.

#### Cancel by labels with `ddel`

The flags to `ddel` can be used in the same way.

To delete all of the above tasks:

```
ddel \
  --label 'billing-code=a9' \
  --label 'batch=august-2017' \
  --status '*' \
  ...
```

To delete only the second task:

```
dstat \
  --label 'billing-code=a9' \
  --label 'batch=august-2017' \
  --label 'sample-id=sam002' \
  --status '*' \
  ...
```

#### Label restrictions:

Rules for setting labels follow the
[Google Compute Engine Restrictions](https://cloud.google.com/compute/docs/labeling-resources#restrictions):

- You can assign up to 64 labels to each resource.
- Label keys and values must conform to the following restrictions:
  - Keys and values cannot be longer than 63 characters each.
  - Keys and values can only contain lowercase letters, numeric characters,
    and dashes.
  - Label keys must start with a lowercase letter.
  - Label keys cannot be empty.

### Check all of my jobs since <some time>

To view results for jobs associated with your user id, since some point in time,
use the `--age` flag.

For example, the following command will return all jobs started in the last day:

```
./dstat --provider google-v2 --project my-project --status '*' --age 1d
```

The `--age` flags supports the following types of values:

1. `<integer><unit>`
2. `<integer>`

The supported `unit` values are:

* `s`: seconds
* `m`: minutes
* `h`: hours
* `d`: days
* `w`: weeks

For example:

* 60s (60 seconds)
* 30m (30 minutes)
* 12h (12 hours)
* 3d (3 days)

A bare integer value is interpreted as days since the epoch (January 1, 1970).

This allows for the use of the `date` command to generate `--age` values.
The [coreutils date command](https://www.gnu.org/software/coreutils/manual/html_node/Examples-of-date.html)
supports even more flexible date strings:

```
./dstat ... --age "$(date --date="last friday" '+%s')"
```

## Monitoring

By default `dstat` will query job status and exit. However, you can use the
`--wait` flag to have `dstat` poll until job completion.

The following examples shows minute-by-minute progression of 3 tasks

```
$ dstat \
  --provider google-v2 \
  --project my-project \
  --jobs my-job-id \
  --wait --poll-interval 60
Job Name        Task    Status    Last Update
--------------  ------  --------  -------------------
my-job-name     task-3  Pending   2017-04-11 16:20:39
my-job-name     task-2  Pending   2017-04-11 16:20:39
my-job-name     task-1  Pending   2017-04-11 16:20:39

Job Name        Task    Status            Last Update
--------------  ------  ----------------  -------------------
my-job-name     task-3  Pending           2017-04-11 16:20:39
my-job-name     task-2  localizing-files  2017-04-11 16:21:44
my-job-name     task-1  pulling-image     2017-04-11 16:22:04

Job Name        Task    Status            Last Update
--------------  ------  ----------------  -------------------
my-job-name     task-3  Pending           2017-04-11 16:20:39
my-job-name     task-2  running-docker    2017-04-11 16:22:59
my-job-name     task-1  localizing-files  2017-04-11 16:22:11

Job Name        Task    Status            Last Update
--------------  ------  ----------------  -------------------
my-job-name     task-3  localizing-files  2017-04-11 16:23:23
my-job-name     task-2  running-docker    2017-04-11 16:22:59
my-job-name     task-1  running-docker    2017-04-11 16:23:23

Job Name        Task    Status          Last Update
--------------  ------  --------------  -------------------
my-job-name     task-3  running-docker  2017-04-11 16:24:39
my-job-name     task-1  running-docker  2017-04-11 16:23:23

Job Name        Task    Status          Last Update
--------------  ------  --------------  -------------------
my-job-name     task-3  running-docker  2017-04-11 16:24:39

```

## Getting detailed job information

The default output from `dstat` is brief tabular text, fit for display on an
80 character terminal. The number of columns is small and column values may
be truncated for space.

`dstat` also supports a `full` output format. When the `--full` flag is used,
the output automatically changes to [YAML](http://yaml.org/) which is
"a human friendly data serialization standard" and more appropriate for
detailed output.

You can use the `--full` and `--format` parameters together to get the output
you want. `--format` supports the values `json`, `text`, `yaml` and
`provider-json`.

The provider JSON output format (`--format=provider-json`) can be used to
debug jobs by inspecting the provider-specific representation of task data.
Provider data representations change over time and no attempt is made to
maintain consistency between dsub versions.

### Full output (default format YAML)

```
$ dstat \
  --provider google-v2 \
  --project my-project \
  --jobs my-job-id \
  --full
- create-time: '2017-04-11 16:47:06'
  end-time: '2017-04-11 16:51:38'
  inputs:
    INPUT_PATH: gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot2_high_cov_GRCh37_bams/data/NA12878/alignment/NA12878.chrom9.SOLID.bfast.CEU.high_coverage.20100125.bam
  internal-id: operations/OPERATION-ID
  job-id: my-job-id
  job-name: my-job-name
  last-update: '2017-04-11 16:51:38'
  outputs:
    OUTPUT_PATH: gs://my-bucket/path/output
  status: Success
  user-id: my-user
```

Note the `Internal ID` in this example provides the
[Google Pipelines API operation name](https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.operations#Operation.FIELDS.name).

### Full output as tabular text

```
$ dstat \
  --provider google-v2 \
  --project my-project \
  --jobs my-job-id \
  --format text \
  --full
Job ID                                  Job Name        Status    Last Update          Created              Ended                User      Internal ID                                                         Inputs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Outputs
--------------------------------------  --------------  --------  -------------------  -------------------  -------------------  --------  ------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  -------------------------------------------------------
my-job-id                               my-job-name     Success   2017-04-11 16:51:38  2017-04-11 16:47:06  2017-04-11 16:51:38  my-user   operations/OPERATION-ID                                             INPUT_PATH=gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot2_high_cov_GRCh37_bams/data/NA12878/alignment/NA12878.chrom9.SOLID.bfast.CEU.high_coverage.20100125.bam
```

### Full output as JSON

```
$ dstat \
  --provider google-v2 \
  --project my-project \
  --jobs my-job-id \
  --format json \
  --full
[
  {
    "status": "Success",
    "inputs": {
      "INPUT_PATH": "gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot2_high_cov_GRCh37_bams/data/NA12878/alignment/NA12878.chrom9.SOLID.bfast.CEU.high_coverage.20100125.bam"
    },
    "job-name": "my-job-name",
    "outputs": {
      "OUTPUT_PATH": "gs://my-bucket/path/output"
    },
    "create-time": "2017-04-11 16:47:06",
    "end-time": "2017-04-11 16:51:38",
    "internal-id": "operations/OPERATION-ID",
    "last-update": "2017-04-11 16:51:38",
    "user-id": "my-user",
    "job-id": "my-job-id"
  }
]
```

## Viewing logs

Each `dsub` task produces log files whose destination is determined by the `--logging` flag.
See [Logging](https://github.com/DataBiosphere/dsub/blob/master/docs/logging.md)
for more information.

## SSH to the VM

With the `google-v2` and `google-cls-v2` providers, there is no SSH server
running on the
Compute Engine Virtual Machine by default. To start an SSH server, use the
`dsub` command-line flag `--ssh` , which will start an SSH container in the
background and will mount your data disk. This will allow you to inspect the
runtime environment of your job's container in real time.

Note that enabling the `--ssh` flag changes the behavior of how the containers
run. Normally, each container uses a separate isolated process ID (PID)
namespace. With `--ssh` enabled, the containers will all use the same PID
namespace (named "shared").

The SSH container will pick up authentication information from the VM, so to
connect you can use the `gcloud compute ssh` command to establish an SSH
session. Alternatively, you can use the cloud console UI to SSH from the
browser. See [SSH from the browser](https://cloud.google.com/compute/docs/ssh-in-browser).

The VM `instance-name` and `zone` can be found in the `provider-attributes`
section of `dstat ... --full` output. For example:

```
  provider-attributes:
    boot-disk-size: 10
    disk-size: 200
    instance-name: google-pipelines-worker-<hash>
    machine-type: n1-standard-1
    preemptible: false
    regions:
    - us-central1
    zone: us-central1-f
    zones: []
```

Then issue the command:

```
    gcloud compute \
      --project your-project \
      ssh \
      --zone <zone> \
     <instance-name>
```

Or alternatively, [SSH from the browser](https://cloud.google.com/compute/docs/ssh-in-browser).
