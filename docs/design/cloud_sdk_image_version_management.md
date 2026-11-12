# TDD: Cloud SDK image version management

Status: Draft. Related: BENCH-10818, dsub #346 (exit code 127), dsub #336 (gcr.io tag retention).

## Context

The `google-batch` provider runs its helper actions in the Cloud SDK image. The helper actions are
`prepare`, logging, localization and delocalization. They need `python3`, `gcloud storage cp`,
`gcloud storage rsync -r` and `/bin/bash`.

Today the default is a rolling tag, set in `dsub/providers/google_utils.py`:

```
DEFAULT_CLOUD_SDK_IMAGE = 'gcr.io/google.com/cloudsdktool/cloud-sdk:slim'
```

`DSUB_CLOUD_SDK_IMAGE` overrides it. The rolling tag was a deliberate choice (commit `4b3c873`).
`gcr.io` keeps a numbered tag for 1 year and then deletes it. A pinned release breaks when its
tag is deleted. `setup.py` states that the pinning policy does not cover this image.

### Incident (2026-11)

- Jobs failed with exit code 127 about 4 seconds after `start`.
- The `slim` image on the Batch VM (SDK 592.0.0) had no `/usr/bin/python3`, but it still set
  `CLOUDSDK_PYTHON=/usr/bin/python3`. Every `gcloud` call failed.
- dsub 0.5.7 sets `CLOUDSDK_PYTHON` to the bundled Python when `python3` is missing.
- No dsub change caused the failure. No test ran against the new image before customers hit it.
- A local `docker pull` of the same tag still had `python3`. A laptop test can pass while the VM
  image fails.

## Goals

1. An upstream image change cannot break customers without a test failing first.
2. A released dsub version keeps working after one year.
3. A customer can pin or replace the image without a dsub change.

## Non-goals

- Replace the Cloud SDK image with an image that dsub builds.
- Change the user's own container image (`--image`).

## Options

| Option | Upside | Downside |
|---|---|---|
| A. Rolling tag (today) | No retention breakage. No bump work. | Upstream changes break customers with no warning. |
| B. Pinned numbered tag, bumped on a cadence | Reproducible. Bumps are tested first. | `gcr.io` deletes the tag after 1 year. Old releases break. |
| C. Pinned tag, mirrored into a Verily-owned public Artifact Registry | The tag cannot disappear. Reproducible. Bumps are tested first. | Needs a registry owner, a push path and egress cost. |
| D. Thin image that dsub builds | Full control of `python3` and `gcloud`. | Most maintenance. |
| E. Rolling tag plus a scheduled canary | Early warning. Small change. | It detects a break but does not stop it. |

A fails goal 1. B fails goal 2. D costs more than the problem needs. E alone fails goal 1.

## Decision

Option C, with the canary from E.

1. **Mirror.** Copy one pinned `cloud-sdk` tag, by digest, to a repo in a Verily-owned public
   Artifact Registry (`crane copy`, so the digest does not change). The repo never deletes tags.
   A public repo is readable without authentication. It must use a `pkg.dev` or `gcr.io` host,
   because `--use-private-address` accepts only those hosts.
2. **Default.** `DEFAULT_CLOUD_SDK_IMAGE` points to the mirror with a numbered tag.
   `DSUB_CLOUD_SDK_IMAGE` stays as the override. `docs/` names it.
3. **Bump.** Run the bump about every 3 months, or sooner for a security fix or a break.
   A bump runs the gating tests against the candidate tag. The new pin is merged only if they pass.
4. **Canary.** A scheduled job runs the same gating tests against the upstream rolling tag. A
   failure alerts the owner before the next bump.
5. **Policy text.** Update the comments in `setup.py` and `google_utils.py`. They now say pinning
   is harmful. After this change the image is pinned, and the mirror removes the retention risk.

### Gating tests

- They run on a real Batch VM. Do not use `docker run` on a laptop.
- They cover each action in the helper list above: script decode, log upload, localize and
  delocalize of files and directories, and the flags `--no-user-output-enabled`, `--content-type`
  and `--billing-project`.
- Also run with `--use-private-address` to prove the image host is accepted.

The exact test subset is open (see below).

## Risks

- **Public egress.** The repo owner pays for pulls. Use a multi-region location close to the Batch
  VMs, or accept the cost.
- **VPC-SC.** Customers with a perimeter must allow the `pkg.dev` host.
- **Mirror outage.** If the mirror is down, jobs fail to pull. `DSUB_CLOUD_SDK_IMAGE` is the
  workaround, and `docs/troubleshooting.md` should say so.
- **Stale pin.** A pin that is never bumped carries known CVEs. The cadence covers this.

## Open items

- Which registry project hosts the mirror, who owns it, and which service account pushes to it.
- Who owns the bump and the canary, and who gets the alert.
- Which tests are the gating set, and which project, network and service account run them.
- Image path: `cloud-sdk` (dsub today) or `google-cloud-cli` (named in the upstream README).
- Region or multi-region for the mirror.
- Record the image digest in job metadata, so a failure maps to one image. Follow-up work.
- Does `google-cls-v2` use the same mirror, or only `google-batch`?
- Does dsub in Workbench apps use the same default?

## Rollout

1. Create the mirror repo and push the first pinned tag.
2. Build the gating test job and the canary.
3. Change the default to the mirror in a dsub release.
4. Document `DSUB_CLOUD_SDK_IMAGE` and the bump process.
