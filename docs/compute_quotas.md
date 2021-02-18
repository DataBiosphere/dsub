# Compute Quotas

When using the `local` provider, `dsub` launches tasks on your local machine and runs
as-is. No attempt is made to constrain resources used or ensure capacity.

When using the Google providers, you can specify the compute resources that your job
tasks will need. For more details on how to specify resources, see
[Compute Resources](./compute_resources.md).
Compute Engine *"enforces quotas on resource usage to prevent abuse and
accidental usage, and to protect users from undesirable effects of other
accounts."*
This document provides details to help you manage this quota for your `dsub`
jobs.

## Background

When you submit a `dsub` job using one of the Google providers, the single
implicit task (for jobs that do not use a `--tasks` file) or the set of tasks
submited (for jobs that do use a `--tasks` file) are submitted to the
[Cloud Life Sciences pipelines.run() API](https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.pipelines/run).
The API maintains a queue of
[operations](https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.operations)
to run. Each job task is a separate operation.

Each operation runs on a newly created
[Compute Engine VM](https://cloud.google.com/compute/docs/instances).
In order to create a new VM, the
[Cloud Life Sciences API](https://cloud.google.com/life-sciences/docs/reference/rest)
submits a request to the Compute Engine API. The Compute Engine API may respond
that your Cloud project has insufficient
[Resource Quotas](https://cloud.google.com/docs/quota) in the region you
have designated for the task to run in.

If the lack of sufficient quota is transient (you have other VMs currently
using quota), then the Life Sciences API will simply retry the VM creation
on a periodic basis until it succeeds.

If the lack of sufficient quota is not transient (the VM requires more resources
than your quota maximum), then the Life Sciences API will mark the operation
as failed and provide an informative message.

## Handling insufficent quota

When you have insufficient quota to run your job tasks, you have a few options:

1. wait for more quota to become available (if the issue is transient)
2. run the job in multiple regions (if you have additional quota in that region)
3. request more quota

### Wait for more quota to become available

Running jobs on Google Cloud allows for running concurrent tasks at a very
high scale. However every Cloud project has a set of global and regional
[Resource Quotas](https://cloud.google.com/compute/quotas). Such quotas
typically start low and can be increased by making a
[quota request](https://cloud.google.com/docs/quota#managing_your_quota).

When you submit a large number of concurrent tasks, you may find that only
a subset of tasks are actually running.

> **_NOTE:_** For historical reasons `dsub --summary` and `dstat` list tasks
that are *queued* as RUNNING.
See [issues/204](https://github.com/DataBiosphere/dsub/issues/204)
for a feature request to distinguish queued vs. running tasks.

As existing tasks finish, their VMs will be deleted, freeing up quota for
new tasks to run. It *can* be more cost effective (with the tradeoff of time
to completion) to run fewer concurrent VMs. Google Compute Engine provides
[Sustained Use Discounts](https://cloud.google.com/compute/docs/sustained-use-discounts)
and VMs of the same shape which run sequentially (not concurrently) can be
inferred as the same VM for discounting purposes.

### Run the job in multiple regions

If the Compute Engine region in which your VM runs does not matter to you,
you can run such tasks in a different region (by specifying different
`--regions` or `--zones` on the `dsub` command-line).

> **_NOTE:_** Copying data across regions can incur egress charges.
For example, if you have data in a Cloud Storage `us-central1` regional bucket
and your Compute Engine VMs run in `us-east1`, then you will incur egress
charges for copying data between the bucket and the VM.
See [Network Egress Pricing](https://cloud.google.com/storage/pricing#network-pricing)
for more details.

### Request more quota

If you would like to run more jobs concurrently, you can make a request
to Google Cloud to
[increase quota](https://cloud.google.com/docs/quota#managing_your_quota).

## What quota is relevant?

Creating a new Compute Engine VM may require quota to be available from
the following `regional` quota limits:

- CPU
  - CPUs
  - C2 CPUs
  - N2 CPUs
  - N2D CPUs

- GPU
  - NVIDIA K80 GPUs
  - NVIDIA P100 GPUs
  - NVIDIA P100 Virtual Workstation GPUs
  - NVIDIA V100 GPUs
  - NVIDIA P4 GPUs
  - NVIDIA P4 Virtual Workstation GPUs

- Disk
  - Persistent Disk Standard
  - Persistent Disk SSD
  - Local SSD

- Network
  - In-use IP addresses

Most commonly, the quotas relevant for `dsub` tasks are:

  - CPUs
  - Persistent Disk Standard
  - In-use IP addresses


> **_NOTE:_** To eliminate dependence on the `In-use IP addresses` quota,
> the Google providers support the `--use-private-address` flag.
> See the `Public IP addresses` section of
> [Compute Resources](https://github.com/DataBiosphere/dsub/blob/main/docs/compute_resources.md).


## Troubleshooting Quota issues

You can view your Compute Engine Quota settings and usage in the Cloud Console
or using the `gcloud` command as described in
[Resource quotas](https://cloud.google.com/compute/quotas).

For example, if you run workflows in `us-central1`, you can see all of your
regional quota with:

```
$ gcloud compute regions describe us-central1
```

to view a specific resource, such as `CPUS`, you can check:

```
$ gcloud compute regions describe us-central1 \
  | grep --context 1 -w CPUS
- limit: 72.0
  metric: CPUS
  usage: 8.0
```
