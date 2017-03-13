# Decompress with dsub

This example demonstrates how to easily decompress files stored in a Google
Cloud Storage bucket by submitting a simple command from a shell prompt
on your laptop. The job executes in the cloud. As input, we start with a list
of compressed variant call format (VCF) files from the
[1000 Genomes Project](http://www.internationalgenome.org/)
that are stored in a public bucket,
[gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/](https://console.cloud.google.com/storage/browser/genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/):

*   20130723_phase3_wg/cornell/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf.gz
*   20140708_previous_phase3/v2_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.vcf.gz
*   20140708_previous_phase3/v1_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.vcf.gz
*   20110721_exome_call_sets/bcm/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.vcf.gz

## Set up

* Follow the [Setup](../../README.md#setup) instructions.

## Decompress one file

### Submit the job

The following command will submit a job to decompress the first input file
from the list above and write the decompressed file to a Cloud Storage bucket
you have write access to.

Set MY-PROJECT to your Cloud project name, and set MY-BUCKET-PATH to your
Cloud bucket and folder.

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging gs://MY-BUCKET-PATH/logging/ \
  --disk-size 200 \
  --image ubuntu:14.04 \
  --input INPUT_VCF="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20130723_phase3_wg/cornell/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf.gz" \
  --output OUTPUT_VCF="gs://MY-BUCKET-PATH/output/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf" \
  --command 'gunzip ${INPUT_VCF} && \
             mv ${INPUT_VCF%.gz} $(dirname ${OUTPUT_VCF})' \
  --wait
```

When you run this command, you should see output like:

```
Job: gunzip--<userid>--170224-114336-37
Launched job-id: gunzip--<userid>--170224-114336-37
        user-id: <userid>
Waiting for jobs to complete...
```

when the job has completed, `dsub` will exit.

### Check the results

To list the output, use the command:

```
gsutil ls gs://MY-BUCKET-PATH/output
```

Output should look like:

```
gs://MY-BUCKET-PATH/output/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf
```

To see the first few lines of the decompressed file, run:

```
gsutil cat gs://MY-BUCKET-PATH/output/*.vcf | head -n 5
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

### Create a TSV file

Open an editor and create a file `submit_list.tsv`:

```
--input INPUT_VCF  --output OUTPUT_VCF
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20140708_previous_phase3/v2_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.vcf.gz  gs://MY-BUCKET-PATH/output/*.vcf
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20140708_previous_phase3/v1_vcfs/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.vcf.gz  gs://MY-BUCKET-PATH/output/*.vcf
gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20110721_exome_call_sets/bcm/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.vcf.gz  gs://MY-BUCKET-PATH/output/*.vcf
```

Set MY-BUCKET-PATH to your bucket and path.

The first line of the file lists the input and output parameter names.
Each subsequent line lists the parameter values.
Replace MY-BUCKET-PATH on each line with your bucket and path.

Note that for the output parameter, for simplicity, we used wildcards to match
the 1 VCF file each task outputs instead of explicitly listing the complete
output file name.

### Submit the job

```
dsub \
  --project MY-PROJECT \
  --zones "us-central1-*" \
  --logging MY-BUCKET-PATH/logging/ \
  --disk-size 200 \
  --image ubuntu:14.04 \
  --table submit_list.tsv \
  --command 'gunzip ${INPUT_VCF} && \
             mv ${INPUT_VCF%.gz} $(dirname ${OUTPUT_VCF})' \
  --wait
```

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
gsutil ls gs://MY-BUCKET-PATH/output
```

Output should look like:

```
gs://MY-BUCKET-PATH/output/ALL.BCM_Illumina_Mosaik_ontarget_plus50bp_822.20110521.snp.exome.genotypes.vcf
gs://MY-BUCKET-PATH/output/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf
gs://MY-BUCKET-PATH/output/ALL.chr21.phase3_shapeit2_mvncall_integrated.20130502.genotype.vcf
gs://MY-BUCKET-PATH/output/ALL.chr21.phase3_shapeit2_mvncall_integrated_v2.20130502.genotypes.vcf
```

