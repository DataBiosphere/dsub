# Run custom scripts with dsub

This example demonstrates how to run your own scripts using dsub.
The same task is demonstrated twice, using:

1. A Bash shell script
2. A Python script

This example takes on a fairly straight-forward task:

* Process a variant call format (VCF) file (compressed or uncompressed)
  * Extract the `#CHROM` header
  * Write all of the sample IDs from the header to a file

This is just a few lines of Bash or Python code. This is quick and easy to run
if you have a single VCF file to process on your local machine.
But what if you have many VCF files in cloud
storage and need to pull out all of the sample IDs?

As input, we start with a single compressed VCF file from the
[1000 Genomes Project](http://www.internationalgenome.org/).

We then proceed to an example that demonstrates processing multiple files,
using a small list of VCFs.
All of the source VCF files are stored in a public bucket at
[gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/](https://console.cloud.google.com/storage/browser/genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/):

*   20130723_phase3_wg/cornell/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf.gz
*   20140708_previous_phase3/v2_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.vcf.gz
*   20140708_previous_phase3/v1_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.vcf.gz
*   20110721_exome_call_sets/bcm/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.vcf.gz

## Set up

* Follow the [dsub geting started](../../README.md#getting-started)
instructions.

## Process one file with a Bash shell script

### Submit the job

The following command will submit a job where the custom VCF processing is done
by a user-provided [shell script](get_vcf_sample_ids.sh).

The dsub backend will:

1. copy the specified input file from cloud storage
2. set an environment variable, `INPUT_VCF`, to the location of the file on
local disk
3. run the shell script which reads from the `INPUT_VCF` and writes output to
a local file whose path is given by the `OUTPUT_FILE` environment variable
4. copy the file to the target location in Cloud Storage

To run a Bash script to decompress the VCF file, type:

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging gs://MY-BUCKET/get_vcf_sample_ids.sh/logging \
  --disk-size 200 \
  --image ubuntu:14.04 \
  --input INPUT_VCF="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20130723_phase3_wg/cornell/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf.gz" \
  --output OUTPUT_FILE="gs://MY-BUCKET/get_vcf_sample_ids.sh/output/sample_ids.txt" \
  --script ./examples/custom_scripts/get_vcf_sample_ids.sh \
  --wait
```

Set MY-PROJECT to your cloud project name, and set MY-BUCKET to a cloud bucket
on which you have write privileges.

You should see output like:

```
Job: get-vcf-sa--<userid>--170327-173245-02
Launched job-id: get-vcf-sa--<userid>--170327-173245-02
        user-id: <userid>
Waiting for job to complete...
Waiting for: get-vcf-sa--<userid>--170327-173245-02.
```

Because the `--wait` flag was set, `dsub` will block until the job completes.

### Check the results

To list the output, use the command:

```
gsutil ls gs://MY-BUCKET/get_vcf_sample_ids.sh/output
```

Output should look like:

```
gs://MY-BUCKET/get_vcf_sample_ids.sh/output/sample_ids.txt
```

To see the first few lines of the sample IDs file, run:

```
gsutil cat gs://MY-BUCKET/get_vcf_sample_ids.sh/output/sample_ids.txt | head -n 5
```

Output should look like:

```
HG00105
HG00107
HG00115
HG00145
HG00157
```

## Process one file with a Python script

### Submit the job

The following command will submit a job where the custom VCF processing is done
by a user-provided [Python script](get_vcf_sample_ids.py).

The dsub backend will:

1. copy the specified input file from cloud storage
2. set an environment variable, `INPUT_VCF`, to the location of the file on
local disk
3. run the Python script which reads from the `INPUT_VCF` and writes output to
a local file whose path is given by the `OUTPUT_FILE` environment variable
4. copy the file to the target location in Cloud Storage

To run a Python script to decompress the VCF file, type:



```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging gs://MY-BUCKET/get_vcf_sample_ids.py/logging \
  --disk-size 200 \
  --image python:2.7 \
  --input INPUT_VCF="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20130723_phase3_wg/cornell/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf.gz" \
  --output OUTPUT_FILE="gs://MY-BUCKET/get_vcf_sample_ids.py/output/sample_ids.txt" \
  --script ./examples/custom_scripts/get_vcf_sample_ids.py \
  --wait
```

Set MY-PROJECT to your cloud project name, and set MY-BUCKET to a cloud bucket
on which you have write privileges.

You should see output like:

```
Job: get-vcf-sa--<userid>--170327-174552-93
Launched job-id: get-vcf-sa--<userid>--170327-174552-93
        user-id: <userid>
Waiting for job to complete...
Waiting for: get-vcf-sa--<userid>--170327-174552-93.
```

Because the `--wait` flag was set, `dsub` will block until the job completes.

### Check the results

To list the output, use the command:

```
gsutil ls gs://MY-BUCKET/get_vcf_sample_ids.py/output
```

Output should look like:

```
gs://MY-BUCKET/get_vcf_sample_ids.py/output/sample_ids.txt
```

To see the first few lines of the sample IDs file, run:

```
gsutil cat gs://MY-BUCKET/get_vcf_sample_ids.py/output/sample_ids.txt | head -n 5
```

Output should look like:

```
HG00105
HG00107
HG00115
HG00145
HG00157
```

## Process a list of files

`dsub` allows you to define a batch of tasks to submit together using a
tab-separated values (TSV) file. Each line lists the inputs and outputs for a
separate task.

More on dsub batch jobs can be found in the
[README](../../README#submitting-a-batch-job).

### Create a TSV file

Open an editor and create a file `submit_list.tsv`:

<pre>
--input INPUT_VCF&#9;--output OUTPUT_FILE
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20140708_previous_phase3/v2_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.vcf.gz&#9;gs://MY-BUCKET/get_vcf_sample_ids/output/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.txt
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20140708_previous_phase3/v1_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.vcf.gz&#9;gs://MY-BUCKET/get_vcf_sample_ids/output/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.txt
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20110721_exome_call_sets/bcm/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.vcf.gz&#9;gs://MY-BUCKET/get_vcf_sample_ids/output/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.txt
</pre>

The first line of the file lists the input and output parameter names.
Each subsequent line lists the parameter values.
Replace MY-BUCKET with a cloud bucket on which you have write privileges.

### Submit the job

Run either of the following commands:

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging gs://MY-BUCKET/get_vcf_sample_ids/logging/ \
  --disk-size 200 \
  --image ubuntu:14.04 \
  --script ./examples/custom_scripts/get_vcf_sample_ids.sh \
  --tasks submit_list.tsv \
  --wait
```

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging gs://MY-BUCKET/get_vcf_sample_ids/logging/ \
  --disk-size 200 \
  --image python:2.7 \
  --script ./examples/custom_scripts/get_vcf_sample_ids.py \
  --tasks submit_list.tsv \
  --wait
```

Output should look like:

```
Job: get-vcf-sa--<userid>--170328-165922-73
Launched job-id: get-vcf-sa--<userid>--170328-165922-73
        user-id: <userid>
  Task: task-1
  Task: task-2
  Task: task-3
Waiting for job to complete...
Waiting for: get-vcf-sa--<userid>--170328-165922-73.
```

When all tasks for the job have completed, `dsub` will exit.

### Check the results

To list the output objects, use the command:

```
gsutil ls gs://MY-BUCKET/get_vcf_sample_ids/output
```

Output should look like:

```
gs://MY-BUCKET/get_vcf_sample_ids/output/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.txt
gs://MY-BUCKET/get_vcf_sample_ids/output/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.txt
gs://MY-BUCKET/get_vcf_sample_ids/output/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.txt
```

