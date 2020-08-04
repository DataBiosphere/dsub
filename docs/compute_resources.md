# Compute Resources

For the `local` provider, `dsub` launches tasks on your local machine and runs
as-is. No attempt is made to constrain resources used or ensure capacity.

For the Google providers, you can specify the compute
resources that your job tasks will need.

## Cores and RAM

By default, `dsub` launches a Compute Engine VM with a single CPU core and
3.75 GB.

Note: There is inconsistency in technology regarding the use of `GB` as a unit.
For more background see [Gibibyte](https://en.wikipedia.org/wiki/Gibibyte).

Google Compute Engine treats `GB` as a base-2 value, where:

- 1GB = 2^30 bytes
- 1GB = 1024 MB

Unit handling by the `dsub` Google providers is consistent
with Google Compute Engine.

### `--min-ram` and `--min-cores` flags

To change the virtual machine minimum RAM, use the `--min-ram` flag.
To change the virtual machine minimum number of CPU cores, use the `--min-cores` flag.

If specified, `dsub` will choose the smallest matching
[custom machine type](https://cloud.google.com/compute/docs/machine-types#custom_machine_types)
that fits these
[specifications](https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type#specifications).

### With the `--machine-type` flags

To explicitly set the virtual machine RAM and number of CPU cores, use the
`--machine-type` flag.

The `--machine-type` value can be one of the
[predefined machine types](https://cloud.google.com/compute/docs/machine-types#predefined_machine_types)
or a
[custom machine type](https://cloud.google.com/compute/docs/machine-types#custom_machine_types).

## Disks

By default, `dsub` launches a Compute Engine VM with a boot disk of 10 GB and an
attached data disk of size 200 GB.

To change the boot disk size, use the `--boot-disk-size` flag.<sup>(\*)</sup>

To change the disk size, use the `--disk-size` flag.

(\*) While the boot disk size *can* be changed, there should be no need to do
so. The provider runtime environment is structured such that you should
only ever need to size the data disk. See the
[provider documentation](providers/README.md) for details.

## Hardware accelerators (GPUs)

You can specify accelerator types (e.g. GPUs) via the `--accelerator-type`
and `--accelerator-count` flags. Please see
https://cloud.google.com/compute/docs/gpus/ for a list of available accelerator
types on Compute Engine.

For example, to use one NVIDIA(R) Tesla(R) K80 GPU:

```
dsub ... \
  --accelerator-type nvidia-tesla-k80 \
  --accelerator-count 1
```

Note that you may only use these types on the
available zones with quota. The default GPU quota is zero on Compute Engine, so
you may need to adjust them by following the instructions at
https://cloud.google.com/compute/quotas.

## Public IP addresses

A Compute Engine VM by default has both a public (external) IP address and a
private (internal) IP address. For batch processing, it is often the case that
no public IP address is necessary. If your job only accesses Google services,
such as Cloud Storage (inputs, outputs, and logging) and Google Container
Registry (your Docker image), then you can run your `dsub` job on VMs without a
public IP address.

For more information on Compute Engine IP addresses, see:

- https://cloud.google.com/compute/docs/ip-addresses

Running the job on VMs without a public IP address has the advantage that it
does not consume `In-use IP addresses` quota, which can otherwise limit your
ability to scale up your concurrently running tasks.
See the [Compute Quotas](https://github.com/DataBiosphere/dsub/blob/master/docs/compute_quotas.md)
documentation for more details.

Running jobs on VMs without a public IP address requires the following:

- Enable "Private Google Access" on your VPC Network subnet
- Ensure your job accesses only Google Services
- Use the `--use-private-address` flag

which are described further below.

Note that if you configure your jobs on VMs without a public IP address and you
use the `--ssh` flag, you will only be able to SSH to those VMs from other VMs
running on the same VPC Network.

### Enable Private Google Access on your VPC Network subnet

New Google Cloud Projects have a `default` VPC Network with a `default`
subnet for each Compute Engine region. If this is the case for your Cloud
project, then you should be able to enable `Private Google Access` as instructed
here:

https://cloud.google.com/vpc/docs/configure-private-google-access#config-pga

As an example, if you run all of your jobs in the region `us-central1`,
and do not explicitly set the `--network` or `--subnetwork` on the command line,
then your jobs run in the VPC Network named `default` and in the `us-central1`
subnetwork named `default`. In this case, follow the instructions provided
to enable `Private Google Access` on this subnetwork.

**If you do not enable `Private Google Access`, then VMs launched with no
public IP address can get stuck in startup, unable to communicate with
the Pipelines API service, the task status only reflecting that a worker
was assigned.**

### Ensure your job accesses only Google Services

The default `--image` used for `dsub` tasks is `ubuntu:14.04` which is pulled
from Dockerhub. For VMs that do not have a public IP address, set the `--image`
flag to a Docker image hosted by
[Google Container Registry](https://cloud.google.com/container-registry/docs).
Google provides a set of
[Managed Base Images](https://cloud.google.com/container-registry/docs/managed-base-images)
in Container Registry that can be used as simple replacements for your tasks.

Also ensure that your user `--command` or `--script` does not require access
to the internet. For example, ensure it does not download resource files with
`curl` or `wget`. You can copy such resource files into a Cloud Storage
bucket in your project and set localize the files with the `--input` flag.

### Use the `--use-private-address` flag

With the appropriate subnetwork configured for `Private Google Access` and job
configured to access only Google Services, you should be able to use the
`--use-private-address` flag successfully.

It is highly recommended that you test your job carefully, checking
`dstat ... --full` events and your `--logging` files to ensure that your job
makes progress and runs to completion.
A misconfigured job can hang indefinitely or  until the infrastructure
terminates the task. The Google providers default `--timeout` is 7 days.
