# Input and Output File Handling

An on-premises job scheduler like Grid Engine typically uses a shared file
system. Your Grid Engine scripts can reference files by their paths on the
shared file system.

With dsub, your input files reside in a
[Google Cloud Storage](https://cloud.google.com/storage/) bucket
and your output files will also be copied out to Cloud Storage.

When you submit a job with dsub

* your input files will be automatically copied from bucket paths to local disk
* your code will work on the local file system inside the Docker container
* your output files will be automatically copied from local disk back to bucket
paths.

Rather than giving many options of what disks to allocate and where to
put input and output files, `dsub` is prescriptive.

* All input and output is written to a single data disk mounted at `/mnt/data`.
* All input and output paths mirror the remote storage location with a local
path of the form `/mnt/data/gs/bucket/path`.

Environment variables are made available to your script indicating the
Docker container input and output paths.

There are several common use cases for both input and output, each described
here and demonstrated in this example.

## Input

### 1. Copy a single file from Cloud Storage.

To copy a single file from Cloud Storage, specify the full URL to the file on
the `dsub` command-line:

```
--input INPUT_FILE=gs://bucket/path/file.bam
```

The object at the Cloud Storage path will be copied and made available at
the path `/mnt/data/input/gs/bucket/path/file.bam`.

The Docker container will receive the environment variable:

```
INPUT_FILE=/mnt/data/input/gs/bucket/path/file.bam
```

### 2. Copy a file pattern from Cloud Storage.

To copy a set of files from Cloud Storage, specify the full URL pattern on
the `dsub` command-line:

```
--input INPUT_FILES=gs://bucket/path/*.bam
```

The object(s) at the Cloud Storage path will be copied and made available at
the path `/mnt/data/input/gs/bucket/path`.

---
Note: *The behavior of dsub for input file patterns is changing slightly.
Please read this section carefully if you use input file patterns.*

---

`dsub` originally made available to the Docker container the environment
variable:

```
INPUT_FILES=/mnt/data/input/gs/bucket/path/
```

This is still true, *but will change*.

The file pattern in this example (`*.bam`) is not available to the Docker
container in the environment variable. The new behavior for `dsub` will be to
set the environment variable value to:

```
INPUT_FILES=/mnt/data/input/gs/bucket/path/*.bam
```

To help users transition, there will be a short period in which both behaviors
are available and the new behavior is explicitly enabled with a command-line
flag. Once the new behavior is the default, the command-line flag will go away.

During the transition period, to enable the new behavior, set
`--vars-include-wildcards` on the `dsub` command-line.

`dsub` script code will typically want to tokenize the environment variable
into its constituent path and pattern components. To tokenize the `INPUT_FILES`
variable, the following code:

```
INPUT_FILES_PATH="$(dirname "${INPUT_FILES}")"
INPUT_FILES_PATTERN="$(basename "${INPUT_FILES}")"
```

will set:

```
INPUT_FILES_PATH=/mnt/data/input/gs/bucket/path
INPUT_FILES_PATTERN=*.bam
```

---
Note: If you are only interested in the path, the following notation will
work with both the old and new behavior (it trims the final slash and anything
that follows):

```
INPUT_FILES_PATH="${INPUT_FILES%/*}"
```
---

To process a list of files from a path + wildcard pattern in Bash, a typical
coding pattern is to create an array and iterate over the array.

If you know you don't have spaces in your paths, this can simply be:

```
readonly INPUT_FILE_LIST=( $(ls "${INPUT_FILES_PATH}"/${INPUT_FILES_PATTERN}) )
```

If you might have spaces in your file paths, then you need to take a bit more
care. Here we create a list of files and force Bash to tokenize the list
by newlines (instead of by whitespace):

```
declare INPUT_FILE_LIST="$(ls -1 "${INPUT_FILES_PATH}"/${INPUT_FILES_PATTERN})"
IFS=$'\n' INPUT_FILE_LIST=(${INPUT_FILE_LIST})
readonly INPUT_FILE_LIST
```

---
Note: in all cases, do not quote `${INPUT_FILES_PATTERN}` as that will
suppress wildcard expansion.

---

The following code shows how to iterate over the list of files in the array:

```
for INPUT_FILE in "${INPUT_FILE_LIST[@]}"; do
  # INPUT_FILE will be the full path including the filename
  # If you need the filename alone, use basename:
  INPUT_FILE_NAME="$(basename "${INPUT_FILE}")"

  # If you further want to trim off the file extension, perhaps to construct
  # a new output file name, then use bash suffix subsititution:
  INPUT_FILE_ROOTNAME="${INPUT_FILE_NAME%.*}"

  # Do stuff with the INPUT_FILE environment variables you now have
  ...
done
```

### 3. Copy a directory recursively from Cloud Storage.

To recursively copy a directory from Cloud Storage, use the
`dsub` command-line flag `--input-recursive`.

```
--input-recursive INPUT_PATH=gs://bucket/path
```

The object(s) at the Cloud Storage path will be recursively copied and
made available at the path `/mnt/data/input/gs/bucket/path`.

The Docker container will receive the environment variable:

```
INPUT_PATH=/mnt/data/input/gs/bucket/path
```

## Output

### 1. Copy a single file to Cloud Storage.

To copy a single file to Cloud Storage, specify the full URL to the file on
the `dsub` command-line:

```
--output OUTPUT_FILE=gs://bucket/path/file.bam
```

Then have your script write the output file to
`${OUTPUT_FILE}` within the Docker container.
The file will be automatically copied to Cloud Storage when your script or
command exits with success.

The Docker container will receive the environment variable:

```
OUTPUT_FILE=/mnt/data/output/gs/bucket/path/file.bam
```

### 2. Copy a file pattern to Cloud Storage.

To copy a set of files to Cloud Storage, specify the full URL pattern on
the `dsub` command-line:

```
--output OUTPUT_FILES=gs://bucket/path/*.bam
```

Then have your job write output files to
`${OUTPUT_FILES}` within the Docker container.
All files matching the pattern `/mnt/data/output/gs/bucket/path/*.bam` will be
automatically copied to Cloud Storage when your script or
command exits with success.

The Docker container will receive the environment variable:

```
OUTPUT_FILES=/mnt/data/output/gs/bucket/path/*.bam
```

Typically a job script will have the output file extensions hard-coded, but
if needed it can be parsed from the environment variable. More commonly,
the job script will need the output directory.

To get the output directory and file extension in bash:

```
# This will set OUTPUT_DIR to "/mnt/data/output/gs/bucket/path"
OUTPUT_DIR="$(dirname "${OUTPUT_FILES}")"

# This will set OUTPUT_FILE_PATTERN to "*.bam"
OUTPUT_FILE_PATTERN="$(basename "${OUTPUT_FILES}")"

# This will set OUTPUT_EXTENSION to "bam" using the bash prefix removal
# operator "##", matching the longest pattern up to and including the period.
OUTPUT_EXTENSION="${OUTPUT_FILE_PATTERN##*.}"
```

### 3. Copy a directory recursively to Cloud Storage.

To recursively copy a directory of output to Cloud Storage, use the
`dsub` command-line flag `--output-recursive`:

```
--output-recursive OUTPUT_PATH=gs://bucket/path
```

Then have your job write output files and subdirectories to
`${OUTPUT_PATH}` within the Docker container.
All files and directories under the path `/mnt/data/output/gs/bucket/path`
will be automatically copied to Cloud Storage when your script or
command exits with success.

The Docker container will receive the environment variable:

```
OUTPUT_PATH=/mnt/data/output/gs/bucket/path
```

## Notice

Note about the `--input-recursive` and `--output-recursive` flags

As a getting started convenience, if `--input-recursive` or `--output-recursive`
are used, dsub will automatically check for and, if needed, install the
[Google Cloud SDK](https://cloud.google.com/sdk/docs/) in the Docker container
at runtime (before your script executes).

If you use the recursive copy features, you are encouraged to install gcloud
in your Docker image when you build it in order to avoid the installation at
runtime.

## Unsupported path formats:

* GCS recursive wildcards (**) are not supported
* Wildcards in the middle of a path are not supported
* Output parameters to a directory are not supported, instead:
  * use an explicit wildcard on the filename (such as `gs://mybucket/mypath/*`)
  * use the recursive copy feature
