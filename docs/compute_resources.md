# Compute Resources

For the `local` provider, `dsub` launches tasks on your local machine and runs
as-is. No attempt is made to constrain resources used or ensure capacity.

For the `google` and `google-v2` providers, you can specify the compute
resources that your job tasks will need.

## Cores and RAM

By default, `dsub` launches a Compute Engine VM with a single CPU core and
3.75 GB.

Note: There is inconsistency in technology regarding the use of `GB` as a unit.
For more background see [Gibibyte](https://en.wikipedia.org/wiki/Gibibyte).

Google Compute Engine treats `GB` as a base-2 value, where:

- 1GB = 2^30 bytes
- 1GB = 1024 MB

Unit handling by the `dsub` `google` and `google-v2` providers is consistent
with Google Compute Engine.

### With the `google` provider (the default):

To change the virtual machine minimum RAM, use the `--min-ram` flag.
To change the virtual machine minimum number of CPU cores, use the `--min-cores` flag.

The machine type selected will be the smallest matching VM from the
[predefined machine types](https://cloud.google.com/compute/docs/machine-types#predefined_machine_types).

### With the `google-v2` provider:

To change the virtual machine RAM and number of CPU cores, use the
`--machine-type` flag.

The `--machine-type` value can be one of the
[predefined machine types](https://cloud.google.com/compute/docs/machine-types#predefined_machine_types)
or a
[custom machine type](https://cloud.google.com/compute/docs/machine-types#custom_machine_types).

You may also use the `--min-ram` and/or `--min-cores` flag similarly to the `google` provider above.
If specified, `dsub` will choose the smallest matching
[custom machine type](https://cloud.google.com/compute/docs/machine-types#custom_machine_types)
that fits these
[specifications](https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type#specifications).

## Disks

By default, `dsub` launches a Compute Engine VM with a boot disk of 10 GB and an
attached data disk of size 200 GB.

To change the boot disk size, use the `--boot-disk-size` flag.<sup>(\*)</sup>

To change the disk size, use the `--disk-size` flag.

(\*) While the boot disk size *can* be changed, there should be no need to do
so. The google provider runtime environment is structured such that you should
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
