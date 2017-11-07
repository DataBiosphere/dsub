# Compute Resources

For the `local` provider, `dsub` launches tasks on your local machine and runs
as-is. No attempt is made to constrain resources used or ensure capacity.

For the `google` provider, you can specify the compute resources that your job
tasks will need.

## Cores and RAM

By default, `dsub` launches a Compute Engine VM with a single CPU core and
3.75 GB.

To change the minimum RAM, use the `--min-ram` flag.

To change the minimum number of CPU cores, use the `--min-cores` flag.

The machine type selected will be the smallest matching VM from the
[predefined machine types](https://cloud.google.com/compute/docs/machine-types#predefined_machine_types).

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
