<!--
  README.md — created by `engineering-automation scaffold create-project`.

  Sections wrapped in AUTO:BEGIN / AUTO:END markers are owned by the generator and
  will be overwritten when documentation rendering lands. Do not hand-edit inside a
  marker pair, and do not remove the markers. Everything outside them is yours.

  Anything you write here that a transformation needs in order to be *correct* is in
  the wrong file. Dataset semantics belong in the data contract; business logic
  belongs in the job spec. This file is for the operational context that has no other
  home.

  Replace every TODO before this repository is reviewed.
-->

# payroll_pipeline

> TODO: One sentence. What this data product is, and who depends on it.

## What this pipeline produces

TODO: The datasets this repository owns, and the decision or downstream system each one
serves. Name the consumers. If a dataset has no consumer, say so explicitly — that is
worth knowing.

## Ownership

| | |
| --- | --- |
| Data product | `payroll_pipeline` |
| Owning team | TODO |
| Primary contact | TODO |
| Escalation channel | TODO |

## Upstream dependencies

TODO: The systems this pipeline reads from and who owns each one. Record refresh cadence,
expected arrival time, and the upstream contact in that source's data contract under
`src/payroll_pipeline/contracts/` — not here. This section is for the dependencies that
are political rather than technical: who to talk to, and what breaks when they change
something.

## Service expectations

TODO: When must the output be ready, and what happens downstream if it is late? State the
freshness commitment you are actually making, not the one you hope for. If there is no
commitment, write "none" — an honest "none" is more useful than an aspirational SLA.

## Operations

TODO: The runbook. Where do logs go? How do you re-run a single failed window? What is the
backfill procedure, and who has to be told before you start one? What are the known
failure modes and their first diagnostic step?

<!-- AUTO:BEGIN jobs -->
## Jobs

_No ETL jobs are defined yet._

Define one with:

```bash
engineering-automation scaffold create-job-settings . <etl_job>
```

then fill in the generated `src/payroll_pipeline/jobs/<etl_job>/<etl_job>.yaml` and its
data contract, and run `engineering-automation generate create-job`.
<!-- AUTO:END jobs -->

<!-- AUTO:BEGIN lineage -->
## Lineage

_Rendered from the job specs once at least one job exists._
<!-- AUTO:END lineage -->

<!-- AUTO:BEGIN contracts -->
## Data contracts

_Rendered from `src/payroll_pipeline/contracts/` once at least one contract exists._
<!-- AUTO:END contracts -->

<!-- AUTO:BEGIN quickstart -->
## Getting started

```bash
make install          # pip install -e ".[dev]"
make test             # full suite
make check            # lint + typecheck + test
```

Individual test tiers: `make test-unit`, `make test-integration`, `make test-e2e`.

Copy `.env.example` to `.env` and fill in the values before running anything that touches
AWS.
<!-- AUTO:END quickstart -->

<!-- AUTO:BEGIN deploying -->
## Deploying

`terraform apply` and the catalog DDL are never run from a workstation. Every AWS
resource in this repository is created or modified by GitHub Actions, so two engineers
cannot diverge and every infrastructure change has a reviewable plan and an audit trail.

| # | Step | Where |
| --- | --- | --- |
| 1 | `generate create-job` / `codegen`, commit the result | local |
| 2 | PR into `develop`: quality, validate, `terraform plan` posted as a comment | CI |
| 3 | merge → dev deploys automatically | CI |
| 4 | PR `develop` → `main`: promotion guard, prod plan posted as a comment | CI |
| 5 | merge → approve the `prod` environment | CI (gate) |
| 6 | apply → catalog DDL → wheel and script upload | CI |
| 7 | run the job | AWS |

`make plan` is a local, read-only check (`-backend=false`, no remote state) — useful
for validating syntax before opening a PR, but it cannot show a true diff against real
infrastructure. Trust the plan CI posts on the pull request.

### Repository setup

New repository, once: `engineering-automation scaffold bootstrap-github .` (after
`gh repo create`) creates the `develop` branch, the `dev`/`prod` GitHub Environments,
the deploy/plan role secrets, and the `AWS_REGION` variable that `deploy-dev.yml` /
`deploy-prod.yml` need. It does not set branch protection on `main` — that is the one
step it deliberately leaves for you to configure by hand.
<!-- AUTO:END deploying -->

<!-- AUTO:BEGIN layout -->
## Repository layout

```
src/payroll_pipeline/
├── contracts/          # data contracts — one YAML per dataset (dataset semantics)
├── jobs/               # one subdirectory per ETL job (job spec + generated code)
├── pipeline/           # shared read() / write() used by every job
├── runtime/            # per-run lifecycle: window resolution, SparkSession, run record
└── tests/              # unit / integration / end_to_end
infra/terraform/        # S3, Glue, and IAM for this data product
```
<!-- AUTO:END layout -->

<!-- AUTO:BEGIN changing -->
## How to change things

| To change | Edit | Then run |
| --- | --- | --- |
| What a column means | the data contract in `contracts/` | `generate codegen` |
| What a transformation does | `description` in the job spec | `generate codegen` |
| Which datasets a job reads or writes | `sources` / `sink` in the job spec | `generate create-job` |
| Infrastructure | `infra/terraform/` | `make plan` |

Generated transformation bodies are the one place where hand-editing is expected and
supported: `generate codegen` only fills functions that are still unimplemented, so a
function you have edited is left alone on re-run.
<!-- AUTO:END changing -->
