# OrgX Python SDK release

## Verified status — September 6, 2026

Version 1.1.0 builds and passes its SDK tests. [Publish run 33984268354](https://github.com/useorgx/orgx-sdk-python/actions/runs/33984268354) failed with `invalid-publisher`: PyPI accepted the identity token but found no matching trusted publisher. A verified source release is not a published registry release.

## Required account setup

The owner of the PyPI `orgx` project must register the publisher matching the
reviewed workflow:

| Setting | Value |
| --- | --- |
| GitHub owner | `useorgx` |
| Repository | `orgx-sdk-python` |
| Workflow filename | `publish.yml` |
| GitHub environment | `pypi` |

Configure it in the project's Publishing settings. If the project has not yet
been created, use PyPI's pending-publisher flow for that project name.
The workflow already declares `environment: pypi` and `id-token: write`.
Keep those values; no PyPI API token is needed for this path.

See [existing-project setup](https://docs.pypi.org/trusted-publishers/adding-a-publisher/)
and [new-project setup](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/).
These values come from the reviewed repository workflow; a failure log alone is
not authority to trust a new publisher.

## Resume and verify

After registration, rerun the failed release job once. Its existing preflight
checks package/version availability, then the workflow tests, checks the release
ref against the package version, builds, and publishes.
If the version already exists, inspect its provenance instead of overwriting it
or incrementing the version solely to bypass the check.

Confirm version 1.1.0 in PyPI and install that exact version in an isolated virtual
environment. Check the `orgx_client` import and continuation methods, then run the
scoped live continuation acceptance check before marking publication complete.
Keep source verification, registry publication, and production acceptance as
separate evidence. Do not retry while the publisher registration is unchanged.
