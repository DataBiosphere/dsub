# For Provider Developers

## How to add a provider

- The file `base.py` lists the methods your provider must implement
  and documents what they should do.

- Add your provider to `provider_base.py`'s `get_provider` method.

- Add your provider to the list of valid flags. It's in `dsub.py`'s
  `parse_arguments` methods, section `--providers`. The variable's called
  `choices`.

- Indicate which command-line arguments are *required* for your provider
  by updating the `provider_required_args` dictionary in that same
  method.

## Guarantees to the user's script

A goal of `dsub` is to enable users to submit jobs to different providers
with changes only in command-line arguments. The users runtime scripts should
be able to run unchanged.

To achieve this, we need certain consistent guarantees from providers,
including:

- Input and output parameters should be handled in a manner consistent
  with the
  [Input and Output File Handling](../docs/input_output.md?q=input_output.md)
  documentation.

  The root location of files need not be `/mnt/data` and script developers
  should not expect the root to be `/mnt/data`. Instead all file and directory
  access should be through environment variables set by the runtime.

- The folder for inputs is expected to be writeable. A historical pattern for
  some scripts has been to use the directory where inputs are as a scratch
  working diretory. If your provider must make the input directories read-only
  it may limit portability of existing scripts.

- The environment variable `TMPDIR` should be set explicitly to a directory
  writeable by the user script. If a provider allows users to adjust the
  amount of disk space available to the user script, point the TMPDIR to
  a directory on that disk.

  See [this github issue](https://github.com/googlegenomics/dsub/issues/24)
  for background.

- Copy the user's script to directory that is writeable to the user and
  preserve the script file name. You *could* write the script and mount the
  location read-only, but why? Give the user control should they want to
  write other code to that directory.

  Preserving the file name makes the tool more transparent and debuggable
  and preserving the file extension allows for direct support of non-bash
  scripts (i.e. Python, Ruby, etc.).

- Create an explicit (empty) working directory for the user and set the working
  directory before the user script runs.

Unless you have a compelling reason not to, follow naming conventions
for the Docker container paths used by other providers, such as the
`local` and `google` providers:

  - input: /mnt/data/input
  - output: /mnt/data/output
  - script: /mnt/data/script
  - tmp: /mnt/data/tmp
  - workingdir: /mnt/data/workingdir
