# Terraform infrastructure lanes

Each directory below is an independent Terraform root. They deliberately use a
local backend by default: no root creates a state backend or configures itself
to use one. Do not commit `terraform.tfstate`, provider credentials, or a
real `.tfvars` file.

| Root | Purpose | Default scope |
| --- | --- | --- |
| `bootstrap` | Optional, future Azure Blob state storage | Not consumed automatically by other roots |
| `platform` | Resource group, ADLS Gen2 storage, lake paths, Databricks Premium workspace, and connector identity | Base deployment |
| `databricks` | Workspace-level Unity Catalog storage credential/external location and optional SQL Warehouse | Explicitly gated, never part of base deployment |

## Safe deployment order

1. Agree the tenant, subscription, naming prefix, tags, and a network pattern.
   This code keeps public endpoints enabled on the workspace and storage for the
   synthetic-data demo. They are reachable from any network, subject to
   authentication/RBAC; this configuration makes **no network restriction**
   claim. Anonymous blob access is denied. This synthetic-only posture requires
   G5 security approval before apply. Disabling workspace public access requires
   a Private Link/VNet design; do not simply flip that setting without
   implementing the private network. Storage public access likewise needs an
   approved private endpoint/DNS and deployer-access design before it is
   disabled.
2. Optionally deploy `bootstrap` and have platform/security assign least
   privilege to the state storage. It has HTTPS-only transport, Entra data-plane
   authentication by default, and no HNS requirement. Configure a backend only
   with a local, uncommitted `backend.hcl`; see `bootstrap/backend.hcl.example`.
3. Deploy `platform`. The deployer needs permission to create the listed Azure
   resources and **Storage Blob Data Contributor** on the platform storage
   account in order to create containers/directories using Entra data-plane
   authentication (`storage_use_azuread = true`). Shared keys and anonymous blob
   access stay disabled. The connector itself receives that role only on the
   `lakehouse` container, never `landing`; a future ingestion identity must be
   separately designed and scoped for landing writes.
4. Before using `databricks`, an administrator must confirm an existing Unity
   Catalog metastore is assigned to the workspace and that the applying identity
   has the required workspace/UC permissions. Set
   `unity_catalog_prerequisites_confirmed = true` only then. This root does not
   create or assign an account-level metastore.

The platform root creates `landing` and `lakehouse` filesystems and empty
directory markers for the Phase 0 sources and Bronze/Silver/quarantine/Gold
layout. Actual ingestion paths, tables, jobs, clusters, and DBU compute are
intentionally out of scope. The optional SQL Warehouse is off by default and
has a short auto-stop value when explicitly enabled. Its `PRO` / `2X-Small`
cost model is unverified and must not be substituted with a serverless estimate.
A budget alert is not a spending cap and is omitted until an approved
notification contact exists.

Run local checks per root (with no Azure login):

```powershell
terraform fmt -check -recursive
terraform init -backend=false
terraform validate
```

`init -backend=false` downloads providers but does not configure or contact an
Azure backend. `validate` is schema-only; it is not evidence that Azure RBAC,
Databricks permissions, naming availability, network reachability, or prices
are valid. Use a reviewed plan only after the listed human gates are complete.
