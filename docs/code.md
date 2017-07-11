# Scripts, Commands, and Docker

`dsub` provides a few choices for how to get your code into the Docker
container in order to run:

* --command "shell string"
* --script "script file (Bash, Python, etc.)"
* --image "Docker image"
* --input "path to file in cloud storage"
* --input-recursive "path to directory in cloud storage"

The following explains each option and how to choose which to use.

## --command 'shell string'

By default a `dsub` job runs in the context of an Ubuntu Docker container.
In its simplest form, a dsub call with a `--command` string will execute
the specified Bash command in an Ubuntu container.

For example:

```
dsub ... \
  --env MESSAGE="hello" \
  --command 'echo ${MESSAGE}'
```

will write the message ("hello") to standard output, which will be
captured in the standard output log file.

The `--command` flag can be handy for cases where you want to run
a simple operation, such as compressing or decompressing a file, or performing
a simple file rewrite operation using
[sed](https://www.gnu.org/software/sed/) or
[awk](https://www.gnu.org/software/gawk/).

**Be sure to enclose your `command string` in single quotes and not double
quotes. If you use double quotes, the command will be expanded in your local
shell before being passed to dsub. If your command flag was in double quotes
as:**

    dsub \
        --project my-cloud-project \
        --zones "us-central1-*" \
        --logging gs://my-bucket/logs \
        --env MESSAGE=hello \
        --command "echo ${MESSAGE}"

**Then assuming you did not have the variable `MESSAGE` in your local shell,
the above command would be passed to dsub as "echo "**

For an example that uses `--command`, see the
[decompress](../examples/decompress) example.

## --script "script file"

If you want to run a more complex series of commands, or you want to run a
Python script, you can use the `--script` flag.

For example:

```
dsub ... \
  --input INPUT_VCF=gs://bucket/path/to/my.vcf \
  --output OUTPUT_VCF=gs://bucket/path/to/new.vcf \
  --script "my_custom_script.sh"
```

will make `my.vcf` available as a local file to the script
`my_custom_script.sh` and the location of the VCF file will be available to
the script via the `INPUT_VCF` environment variable.

The location for `my_custom_script.sh` to write the output VCF will be
available via the `OUTPUT_VCF` environment variable.
If the script writes the output VCF to the path given by the `OUTPUT_VCF`
environment variable, the output file will be copied to the output location,
`gs://bucket/path/to/new.vcf`.


For more information on file handling, see the documentation on
[input and output](input_output.md).
For an example that demonstrates using a custom shell script or a custom Python
script, see the [Custom Scripts example](../examples/custom_scripts).

## --image "Docker image"

The `--image` flag allows you to specify the Docker image to be used for running
a job. Many software packages are already available in public Docker images at
sites such as [Docker Hub](https://hub.docker.com/). Images can be pulled
from Docker Hub or any container registry:

```
--image debian:jessie           # pull image implicitly from Docker hub.
--image gcr.io/PROJECT/IMAGE    # pull from GCR registry.
--image quay.io/quay/ubuntu     # pull from Quay.io.
```

When you have more than a single custom script to run or you have dependent
files, you need a way to get them into your Docker container at execution time.
For portability, the recommended way to do this is to build a custom image and
store it in a container registry.

A quick way to start using custom Docker images is to use Google Container
Builder which will build an image remotely and store it in the [Google Container
Registry](https://cloud.google.com/container-registry/docs/). Alternatively you
can build a Docker image locally and push it to a registry. See the
[FastQC example](../examples/fastqc) for a demonstration of both strategies.

For information on building Docker images, see the Docker documentation:

* [Build your own image](https://docs.docker.com/engine/getstarted/step_four/)
* [Best practices for writing Dockerfiles](https://docs.docker.com/engine/userguide/eng-image/dockerfile_best-practices/)
* [Google Cloud Container Builder](https://cloud.google.com/container-builder/docs/)


## --input "path to file in cloud storage"

If your script has dependent files and you'd rather not make a Docker image
as described above, you can copy them to cloud storage, and
use dsub to make them available to your job's runtime environment.

For example, suppose you have 3 scripts, including a driver script that calls
two dependent scripts:

**driver.sh**

```
#!/bin/bash

set -o errexit
set -o nounset

chmod u+x ${SCRIPT1}
chmod u+x ${SCRIPT2}

${SCRIPT1}
${SCRIPT2}
```

**my-code/script1.sh**

```
#!/bin/bash

set -o errexit
set -o nounset

# Do something interesting
```

**my-code/script2.sh**

```
#!/bin/bash

set -o errexit
set -o nounset

# Do something interesting
```

To run the driver script, first copy `script1.sh` and `script2.sh` to
cloud storage:

```
gsutil cp my-code/script1.sh my-code/script2.sh gs://MY-BUCKET/my-code/
```

Then launch a dsub job:

```
dsub ... \
  --input SCRIPT1=gs://MY-BUCKET/my-code/script1.sh \
  --input SCRIPT2=gs://MY-BUCKET/my-code/script2.sh \
  --script driver.sh
```

## --input-recursive "path to directory in cloud storage"

Extending the previous example, you could copy `script1.sh` and `script2.sh`
to cloud storage with:

```
gsutil rsync -r my-code gs://MY-BUCKET/my-code/
```

and then launch a `dsub` job with:

```
dsub ... \
  --input-recursive SCRIPT_DIR=gs://MY-BUCKET/my-code
  --script driver.sh
```

in this case, `driver.sh` would receive an environment variable `SCRIPT_DIR`
and could be written as:

**driver.sh**

```
#!/bin/bash

set -o errexit
set -o nounset

chmod u+x ${SCRIPT_DIR}/*.sh

${SCRIPT_DIR}/script1.sh
${SCRIPT_DIR}/script2.sh
```
