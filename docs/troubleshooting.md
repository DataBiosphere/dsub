# Checking Status and Troubleshooting Jobs

Once jobs are launched using `dsub`, you'll typically want to check on status.
If something goes wrong, you'll want to discover why.

## Checking status

`dstat` allows you to get status information for your jobs and supports
filtering of output based on 3 fields. Each field can take a list of values
to filter on:

* `job-id`: one or more job-id values
* `user-id`: the `$USER` who submitted the job, or `"*"` for all users
* `status`: `RUNNING`, `SUCCESS`, `FAILURE`, `CANCELED`, or `"*"` for all job
statuses

### Check all my running jobs

When submitted with no filter arguments, `dstat` shows  information for all
tasks in the `RUNNING` state belonging to the current user:

```
$ dstat --project my-project
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

### Check one of my jobs

If you are running multiple jobs concurrently, you may want to check status on
them separately. To check on a specific job, pass the `--jobs` (or `-j`)
flag. For example:

```
$ dstat --project my-project --jobs my-job-id
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
$ dstat --project my-project \
  --jobs my-job-id \
  --status "*"
Job Name        Status                          Last Update
--------------  ------------------------------  -------------------
my-job-name     Operation canceled at 2017-...  2017-04-11 16:07:02
```

Be sure to quote the `*` to prevent shell expansion.

### Check all of my jobs

To view results for all jobs associated with your user id:

```
dstat --project my-project --status "*"
```

## Monitoring

By default `dstat` will query job status and exit. However, you can use the
`--wait` flag to have `dstat` poll until job completion.

The following examples shows minute-by-minute progression of 3 tasks

```
$ dstat --project my-project \
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
you want. `--format` supports the values `json`, `text`, and `yaml`.

### Full output (default format YAML)

```
$ dstat --project my-project \
  --jobs my-job-id \
  --full
- create-time: '2017-04-11 16:47:06'
  end-time: '2017-04-11 16:51:38'
  inputs:
    INPUT_PATH: gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot2_high_cov_GRCh37_bams/data/NA12878/alignment/NA12878.chrom9.SOLID.bfast.CEU.high_coverage.20100125.bam
    _SCRIPT: |+
      #!/bin/bash

      # Copyright 2016 Google Inc. All Rights Reserved.
      #
      # Licensed under the Apache License, Version 2.0 (the "License");
<trimmed for brevity>
      readonly INPUT_FILE_LIST="$(ls "${INPUT_PATH}")"

      for INPUT_FILE in "${INPUT_FILE_LIST[@]}"; do
        FILE_NAME="$(basename "${INPUT_FILE}")"

        md5sum "${INPUT_FILE}" | awk '{ print $1 }' > "${OUTPUT_DIR}/${FILE_NAME}.md5"
      done

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
[Google Pipelines API operation name](https://cloud.google.com/genomics/reference/rest/v1alpha2/operations#name).

### Full output as tabular text

```
$ dstat --project my-project \
  --jobs my-job-id \
  --format text \
  --full
Job ID                                  Job Name        Status    Last Update          Created              Ended                User      Internal ID                                                         Inputs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Outputs
--------------------------------------  --------------  --------  -------------------  -------------------  -------------------  --------  ------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  -------------------------------------------------------
my-job-id                               my-job-name     Success   2017-04-11 16:51:38  2017-04-11 16:47:06  2017-04-11 16:51:38  my-user   operations/OPERATION-ID                                             INPUT_PATH=gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot2_high_cov_GRCh37_bams/data/NA12878/alignment/NA12878.chrom9.SOLID.bfast.CEU.high_coverage.20100125.bam, _SCRIPT=#!/bin/bash
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
<trimmed for brevity>
for INPUT_FILE in "${INPUT_FILE_LIST[@]}"; do
  FILE_NAME="$(basename "${INPUT_FILE}")"

  md5sum "${INPUT_FILE}" | awk '{ print $1 }' > "${OUTPUT_DIR}/${FILE_NAME}.md5"
done  OUTPUT_PATH=gs://my-bucket/path/output
```

### Full output as JSON

```
$ dstat --project my-project \
  --jobs my-job-id \
  --format json \
  --full
[
  {
    "status": "Success",
    "inputs": {
      "_SCRIPT": "#!/bin/bash\n\n# Copyright 2016 Google Inc. All Rights Reserved.\n#\n# Licensed under the Apache License, Version 2.0 (the \"License\");\n<trimmed for brevity>for INPUT_FILE in \"${INPUT_FILE_LIST[@]}\"; do\n  FILE_NAME=\"$(basename \"${INPUT_FILE}\")\"\n\n  md5sum \"${INPUT_FILE}\" | awk '{ print $1 }' > \"${OUTPUT_DIR}/${FILE_NAME}.md5\"\ndone\n\n", 
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

The location of `dsub` log files is determined by the `--logging` flag.
The types of logs will vary depending on the `dsub` backend provider.
The Google Pipelines API is currently the only backend provider.
See the
[Pipelines API Troubleshooting guide](https://cloud.google.com/genomics/v1alpha2/pipelines-api-troubleshooting)
for more details on log files.
