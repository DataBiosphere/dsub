# Run FastQC on a list of BAM files with dsub

This example demonstrates how to run FastQC on BAM files stored in a
Google Cloud Storage bucket by submitting a simple command from a shell prompt
on your laptop. The job executes in the cloud. This example also demonstrates
two different methods of creating a Docker image to be used for this job.

Here we will work through two examples. The first example processes a single
binary Sequence Alignment/Map format (BAM) file from the
[1000 Genomes Project](http://www.internationalgenome.org/). The second
example demonstrates processing multiple files, using a small list of BAMs.

All of the source BAM files are stored in a public bucket at
[gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment](https://console.cloud.google.com/storage/browser/genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/):

* NA06986.chrom19.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
* NA06986.chrom20.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
* NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
* NA06986.chrom22.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam


## Setup

* Follow the [dsub geting started](../../README.md#getting-started)
  instructions.

* (Optional) [Enable](https://console.cloud.google.com/flows/enableapi?apiid=cloudbuild.googleapis.com)
  the Google Container Builder API.

  This step is necessary if you are going to build the FastQC Docker image
  remotely.


## Create the Docker image

If you have Docker installed, you can build the FastQC Docker image
locally and push it to Google Container Registry.

If you do not have Docker installed, you can use Google Container
Builder to build the image in the cloud and have it automatically pushed to
Google Container Registry.


### Create the image locally

Use the following command to build the image locally. Substitute in your
project ID. If your project ID is domain-scoped (`example.com:foo-bar`)
you will need to replace the colon with a forward slash
(ex: `example.com/foo-bar`).

```
docker build --tag gcr.io/MY-PROJECT/fastqc ./
gcloud docker -- push gcr.io/MY-PROJECT/fastqc
```

### Create the image with Google Container Builder

This command can be used to build the image remotely and automatically store
it in the Google Container Registry. If your project ID is domain-scoped
(`example.com:foo-bar`) you will need to replace the colon with a forward slash
(ex: `example.com/foo-bar`).

```
 gcloud container builds submit ./ --tag=gcr.io/MY-PROJECT/fastqc
```


## Run FastQC on one BAM

### Submit the job

The following command will submit a job to run FastQC on the first BAM file
from the list above and write the results files to a Cloud Storage bucket
you have write access to.

To run FastQC on the BAM file, type:

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging "gs://MY-BUCKET/fastqc/submit_one/logging" \
  --disk-size 200 \
  --name "fastqc" \
  --image "gcr.io/MY-PROJECT/fastqc" \
  --output OUTPUT_FILES="gs://MY-BUCKET/fastqc/submit_one/output/*" \
  --input INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom19.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam" \
  --command 'fastqc ${INPUT_BAM} --outdir=$(dirname ${OUTPUT_FILES})' \
  --wait
```

Set MY-PROJECT to your cloud project name, and set MY-BUCKET to a cloud bucket
on which you have write privileges.

You should see output like:

```
Job: fastqc--<userid>--170619-105212-67
Launched job-id: fastqc--<userid>--170619-105212-67
To check the status, run:
  dstat --project MY-PROJECT --jobs fastqc--<userid>--170619-105212-67 --status '*'
To cancel the job, run:
  ddel --project MY-PROJECT --jobs fastqc--<userid>--170619-105212-67
Waiting for job to complete...
Waiting for: fastqc--<userid>--170619-105212-67.
```

Because the `--wait` flag was set, `dsub` will block until the job completes.

### Check the results

To list the output, use the command:

```
gsutil ls -l gs://MY-BUCKET/fastqc/submit_one/output
```

Output should look like:

```
    255162  2017-06-20T18:09:28Z  gs://MY-BUCKET/fastqc/submit_one/output/NA06986.chrom19.ILLUMINA.bwa.CEU.exon_targetted.20100311_fastqc.html
    268204  2017-06-20T18:09:28Z  gs://MY-BUCKET/fastqc/submit_one/output/NA06986.chrom19.ILLUMINA.bwa.CEU.exon_targetted.20100311_fastqc.zip
TOTAL: 2 objects, 523366 bytes (511.1 KiB)
```

## Run FastQC on multiple files

`dsub` allows you to define a batch of tasks to submit together using a
tab-separated values (TSV) file listing the inputs and outputs.
Each line lists the inputs and outputs for a separate task.

More on dsub batch jobs can be found in the
[README](../../README#submitting-a-batch-job).

### Create a TSV file

Open an editor and create a file `submit_list.tsv`:

<pre>
--output OUTPUT_FILES&#9;--input INPUT_BAM
gs://MY-BUCKET/fastqc/submit_list/output/*&#9;gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom20.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
gs://MY-BUCKET/fastqc/submit_list/output/*&#9;gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
gs://MY-BUCKET/fastqc/submit_list/output/*&#9;gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom22.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
</pre>

The first line of the file lists the output and input parameter names.
Each subsequent line lists the parameter values.
Replace MY-BUCKET with a Cloud bucket on which you have write privileges.

Note that for the output parameter, for simplicity, we used wildcards to match
the two files that FastQC tasks output instead of explicitly listing each
output file name.

### Submit the job

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging "gs://MY-BUCKET/samtools/submit_list/logging/" \
  --disk-size 200 \
  --name "fastqc" \
  --image "gcr.io/MY-PROJECT/fastqc" \
  --tasks submit_list.tsv \
  --command 'fastqc ${INPUT_BAM} --outdir=$(dirname ${OUTPUT_FILES})' \
  --wait
```

Output should look like:

```
Job: fastqc--<userid>--170522-154943-70
Launched job-id: fastqc--<userid>--170522-154943-70
3 task(s)
To check the status, run:
  dstat --project MY-PROJECT --jobs fastqc--<userid>--170522-154943-70 --status '*'
To cancel the job, run:
  ddel --project MY-PROJECT --jobs fastqc--<userid>--170522-154943-70
Waiting for job to complete...
Waiting for: fastqc--<userid>--170522-154943-70.
```

when all tasks for the job have completed, `dsub` will exit.

### Check the results

To list the output objects, use the command:

```
gsutil ls -l gs://MY-BUCKET/fastqc/submit_list/output
```

Output should look like:

```
    228798  2017-06-20T18:19:09Z  gs://MY-BUCKET/fastqc/submit_list/output/NA06986.chrom20.ILLUMINA.bwa.CEU.exon_targetted.20100311_fastqc.html
    235454  2017-06-20T18:19:09Z  gs://MY-BUCKET/fastqc/submit_list/output/NA06986.chrom20.ILLUMINA.bwa.CEU.exon_targetted.20100311_fastqc.zip
    231242  2017-06-20T18:19:14Z  gs://MY-BUCKET/fastqc/submit_list/output/NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311_fastqc.html
    240432  2017-06-20T18:19:14Z  gs://MY-BUCKET/fastqc/submit_list/output/NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311_fastqc.zip
    249472  2017-06-20T18:19:08Z  gs://MY-BUCKET/fastqc/submit_list/output/NA06986.chrom22.ILLUMINA.bwa.CEU.exon_targetted.20100311_fastqc.html
    260910  2017-06-20T18:19:08Z  gs://MY-BUCKET/fastqc/submit_list/output/NA06986.chrom22.ILLUMINA.bwa.CEU.exon_targetted.20100311_fastqc.zip
TOTAL: 6 objects, 1446308 bytes (1.38 MiB)
```

