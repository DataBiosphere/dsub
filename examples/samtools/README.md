# Index BAM files with dsub

This example demonstrates how to index BAM files stored in a Google
Cloud Storage bucket by submitting a simple command from a shell prompt
on your laptop. The job executes in the cloud.

Here we will work through two examples. The first example processes a single
binary Sequence Alignment/Map format (BAM) file from the
[1000 Genomes Project](http://www.internationalgenome.org/). The second
example demonstrates processing multiple files, using a small list of BAMs.

All of the source BAM files are stored in a public bucket at
[gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA12878/alignment](https://console.cloud.google.com/storage/browser/genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA12878/alignment/):

* NA12878.chrom11.SOLID.corona.SRP000032.2009_08.bam
* NA12878.chrom12.SOLID.corona.SRP000032.2009_08.bam
* NA12878.chrom10.SOLID.corona.SRP000032.2009_08.bam
* NA12878.chromX.SOLID.corona.SRP000032.2009_08.bam

## Set up

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
  --logging gs://MY-BUCKET/samtools/submit_one/logging \
  --disk-size 200 \
  --name "samtools index" \
  --image quay.io/cancercollaboratory/dockstore-tool-samtools-index \
  --input INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA12878/alignment/NA12878.chrom11.SOLID.corona.SRP000032.2009_08.bam" \
  --output OUTPUT_BAI=gs://MY-BUCKET/samtools/submit_one/output/*.bai \
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
    396680  2017-05-22T22:43:15Z  gs://MY-BUCKET/samtools/submit_one/output/NA12878.chrom11.SOLID.corona.SRP000032.2009_08.bam.bai
TOTAL: 1 objects, 396680 bytes (387.38 KiB)
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
--input INPUT_BAM&#9;--output OUTPUT_BAI
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA12878/alignment/NA12878.chrom12.SOLID.corona.SRP000032.2009_08.bam&#9;gs://MY-BUCKET/samtools/submit_list/output/*.bai
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA12878/alignment/NA12878.chrom10.SOLID.corona.SRP000032.2009_08.bam&#9;gs://MY-BUCKET/samtools/submit_list/output/*.bai
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA12878/alignment/NA12878.chromX.SOLID.corona.SRP000032.2009_08.bam&#9;gs://MY-BUCKET/samtools/submit_list/output/*.bai
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
  --logging gs://MY-BUCKET/samtools/submit_list/logging/ \
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
    395600  2017-05-22T22:55:26Z  gs://MY-BUCKET/samtools/submit_list/output/NA12878.chrom10.SOLID.corona.SRP000032.2009_08.bam.bai
    393640  2017-05-22T22:54:35Z  gs://MY-BUCKET/samtools/submit_list/output/NA12878.chrom12.SOLID.corona.SRP000032.2009_08.bam.bai
    454096  2017-05-22T22:55:10Z  gs://MY-BUCKET/samtools/submit_list/output/NA12878.chromX.SOLID.corona.SRP000032.2009_08.bam.bai
TOTAL: 3 objects, 1243336 bytes (1.19 MiB)
```

