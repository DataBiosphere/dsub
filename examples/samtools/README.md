# Index BAM files with dsub

This example demonstrates how to index BAM files stored in a Google
Cloud Storage bucket by submitting a simple command from a shell prompt
on your laptop. The job executes in the cloud.

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

## Index one BAM file

### Submit the job

The following command will submit a job to index the first BAM file
from the list above and write the BAI file to a Cloud Storage bucket
you have write access to.

The command uses a pre-built Docker image [dockstore-tool-samtools-index]
(https://quay.io/repository/cancercollaboratory/dockstore-tool-samtools-index)
which contains [samtools](http://www.htslib.org/).

To run a command to index the BAM file, type:

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging "gs://MY-BUCKET/samtools/submit_one/logging" \
  --disk-size 200 \
  --name "samtools index" \
  --image quay.io/cancercollaboratory/dockstore-tool-samtools-index \
  --input INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom19.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam" \
  --output OUTPUT_BAI="gs://MY-BUCKET/samtools/submit_one/output/*.bai" \
  --command 'export BAI_NAME="$(basename "${INPUT_BAM}").bai"
             samtools index \
               "${INPUT_BAM}" \
               "$(dirname "${OUTPUT_BAI}")/${BAI_NAME}"' \
  --wait
```

Set MY-PROJECT to your cloud project name, and set MY-BUCKET to a cloud bucket
on which you have write privileges.

You should see output like:

```
Job: samtools-i--<userid>--170522-153810-14
Launched job-id: samtools-i--<userid>--170522-153810-14
To check the status, run:
  dstat --project MY-PROJECT --jobs samtools-i--<userid>--170522-153810-14 --status '*'
To cancel the job, run:
  ddel --project MY-PROJECT --jobs samtools-i--<userid>--170522-153810-14
Waiting for job to complete...
Waiting for: samtools-i--<userid>--170522-153810-14.
```

Because the `--wait` flag was set, `dsub` will block until the job completes.

### Check the results

To list the output, use the command:

```
gsutil ls -l gs://MY-BUCKET/samtools/submit_one/output
```

Output should look like:

```
    111784  2017-06-20T21:06:53Z  gs://MY-BUCKET/samtools/submit_one/output/NA06986.chrom19.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam.bai
TOTAL: 1 objects, 111784 bytes (109.16 KiB)
```

## Index multiple files

`dsub` allows you to define a batch of tasks to submit together using a
tab-separated values (TSV) file listing the inputs and outputs.
Each line lists the inputs and outputs for a separate task.

More on dsub batch jobs can be found in the
[README](../../README#submitting-a-batch-job).

### Create a TSV file

Open an editor and create a file `submit_list.tsv`:

<pre>
--output OUTPUT_BAI&#9;--input INPUT_BAM
gs://MY-BUCKET/samtools/submit_list/output/*.bai&#9;gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom20.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
gs://MY-BUCKET/samtools/submit_list/output/*.bai&#9;gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
gs://MY-BUCKET/samtools/submit_list/output/*.bai&#9;gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom22.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
</pre>

The first line of the file lists the input and output parameter names.
Each subsequent line lists the parameter values.
Replace MY-BUCKET with a Cloud bucket on which you have write privileges.

Note that for the output parameter, for simplicity, we used wildcards to match
the 1 BAI file each task outputs instead of explicitly listing the complete
output file name.

### Submit the job

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging "gs://MY-BUCKET/samtools/submit_list/logging/" \
  --disk-size 200 \
  --name "samtools index" \
  --image quay.io/cancercollaboratory/dockstore-tool-samtools-index \
  --tasks submit_list.tsv \
  --command 'export BAI_NAME="$(basename "${INPUT_BAM}").bai"
             samtools index \
               "${INPUT_BAM}" \
               "$(dirname "${OUTPUT_BAI}")/${BAI_NAME}"' \
  --wait
```

Output should look like:

```
Job: samtools-i--<userid>--170522-154943-70
Launched job-id: samtools-i--<userid>--170522-154943-70
3 task(s)
To check the status, run:
  dstat --project MY-PROJECT --jobs samtools-i--<userid>--170522-154943-70 --status '*'
To cancel the job, run:
  ddel --project MY-PROJECT --jobs samtools-i--<userid>--170522-154943-70
Waiting for job to complete...
Waiting for: samtools-i--<userid>--170522-154943-70.
```

when all tasks for the job have completed, `dsub` will exit.

### Check the results

To list the output objects, use the command:

```
gsutil ls -l gs://MY-BUCKET/samtools/submit_list/output
```

Output should look like:

```
    111240  2017-06-20T18:21:20Z  gs://MY-BUCKET/samtools/submit_list/output/NA06986.chrom20.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam.bai
     69984  2017-06-20T18:22:22Z  gs://MY-BUCKET/samtools/submit_list/output/NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam.bai
     76216  2017-06-20T18:21:51Z  gs://MY-BUCKET/samtools/submit_list/output/NA06986.chrom22.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam.bai
TOTAL: 3 objects, 257440 bytes (251.41 KiB)
```

