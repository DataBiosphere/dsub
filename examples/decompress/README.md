# Decompress with dsub

This example demonstrates how to decompress files stored in a Google
Cloud Storage bucket by submitting a simple command from a shell prompt
on your laptop. The job executes in the cloud. As input, we start with a
single compressed variant call format (VCF) file from the
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

## Decompress one file

### Submit the job

The following command will submit a job to decompress the first input file
from the list above and write the decompressed file to a Cloud Storage bucket
you have write access to.

To run a command to decompress the VCF file, type:

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging gs://MY-BUCKET/decompress_one/logging/ \
  --disk-size 200 \
  --image ubuntu:14.04 \
  --input INPUT_VCF="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20130723_phase3_wg/cornell/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf.gz" \
  --output OUTPUT_VCF="gs://MY-BUCKET/decompress_one/output/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf" \
  --command 'gunzip ${INPUT_VCF} && \
             mv ${INPUT_VCF%.gz} $(dirname ${OUTPUT_VCF})' \
  --wait
```

Set MY-PROJECT to your cloud project name, and set MY-BUCKET to a cloud bucket
on which you have write privileges.

You should see output like:

```
Job: gunzip--<userid>--170224-114336-37
Launched job-id: gunzip--<userid>--170224-114336-37
        user-id: <userid>
Waiting for jobs to complete...
```

Because the `--wait` flag was set, `dsub` will block until the job completes.

### Check the results

To list the output, use the command:

```
gsutil ls gs://MY-BUCKET/decompress_one/output
```

Output should look like:

```
gs://MY-BUCKET/decompress_one/output/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf
```

To see the first few lines of the decompressed file, run:

```
gsutil cat gs://MY-BUCKET/decompress_one/output/*.vcf | head -n 5
```

Output should look like:

```
##fileformat=VCFv4.1
##FILTER=<ID=LowQual,Description="Low quality">
##FILTER=<ID=PASS,Description="Passing basic quality fiters">
##FORMAT=<ID=AD,Number=.,Type=Integer,Description="Allelic depths for the ref and alt alleles in the order listed">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Approximate read depth (reads with MQ=255 or with bad mates are filtered)">
```

## Decompress multiple files

`dsub` allows you to define a batch of tasks to submit together using a
tab-separated values (TSV) file listing the inputs and outputs.
Each line lists the inputs and outputs for a separate task.

More on dsub batch jobs can be found in the
[README](../../README#submitting-a-batch-job).

### Create a TSV file

Open an editor and create a file `submit_list.tsv`:

<pre>
--input INPUT_VCF&#9;--output OUTPUT_VCF
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20140708_previous_phase3/v2_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.vcf.gz&#9;gs://MY-BUCKET/decompress_list/output/*.vcf
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20140708_previous_phase3/v1_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.vcf.gz&#9;gs://MY-BUCKET/decompress_list/output/*.vcf
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20110721_exome_call_sets/bcm/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.vcf.gz&#9;gs://MY-BUCKET/decompress_list/output/*.vcf
</pre>

The first line of the file lists the input and output parameter names.
Each subsequent line lists the parameter values.
Replace MY-BUCKET with a Cloud bucket on which you have write privileges.

Note that for the output parameter, for simplicity, we used wildcards to match
the 1 VCF file each task outputs instead of explicitly listing the complete
output file name.

### Submit the job

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging gs://MY-BUCKET/decompress_list/logging/ \
  --disk-size 200 \
  --image ubuntu:14.04 \
  --command 'gunzip ${INPUT_VCF} && \
             mv ${INPUT_VCF%.gz} $(dirname ${OUTPUT_VCF})' \
  --tasks submit_list.tsv \
  --wait
```

Output should look like:

```
Job: gunzip--<userid>--170224-122223-54
Launched job-id: gunzip--<userid>--170224-122223-54
        user-id: <userid>
  Task: task-1
  Task: task-2
  Task: task-3
Waiting for jobs to complete...
```

when all tasks for the job have completed, `dsub` will exit.

### Check the results

To list the output objects, use the command:

```
gsutil ls gs://MY-BUCKET/decompress_list/output
```

Output should look like:

```
gs://MY-BUCKET/decompress_list/output/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.vcf
gs://MY-BUCKET/decompress_list/output/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf
gs://MY-BUCKET/decompress_list/output/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.vcf
gs://MY-BUCKET/decompress_list/output/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.vcf
```

