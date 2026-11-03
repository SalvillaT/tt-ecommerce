# Terraform infrastructure lanes

Each directory below is an independent Terraform root. They deliberately use a
local backend by default: no root creates a state backend or configures itself
to use one. Do not commit `terraform.tfstate`, provider credentials, or a
real `.tfvars` file.

| Root | Purpose | Default scope |
| --- | --- | --- |
| `bootstrap` | Optional, future Azure Blob state storage | Not consumed automatically by other roots |
| `platform` | Resource group, ADLS Gen2 storage, lake paths, Databricks Premium workspace, and connector identity | Base deployment |
| `databricks` | Phase 1 workspace-level UC credential/external location, synthetic catalog, Bronze/Silver/quarantine/Gold schemas, and minimal writer grants; optional Phase 5 SQL Warehouse | Explicitly gated; SQL is off by default |

The planned region for `platform` and optional `bootstrap` is **France Central**. A user-observed West Europe portal warning is not evidence that France Central supports this subscription, Free Trial quota, or Databricks Premium. No resources are deployed. For the approved future-session lifecycle (keep `platform`; stop active compute rather than destroy infrastructure), see [Fase 1 development cycle](../docs/fase-1/CICLO_DE_DESARROLLO.md). It is preventive documentation only: nothing is deployed or active, and the Phase 1 gates still govern any provider operation.

## Safe deployment order

1. Agree France Central, the tenant, subscription, naming prefix, tags, and a network pattern; recheck regional quota/Premium eligibility and persistent workspace/ADLS cost, budget, alerts, and owner. Do not use Portal **Create** instead of the reviewed Terraform roots.
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
   Catalog metastore is assigned to the workspace, the applying identity has the
   required workspace/UC role and object-owner authority, and the metastore has
   an approved **DEFAULT** managed-storage location. Set
   `unity_catalog_prerequisites_confirmed = true` and
   `metastore_default_managed_storage_confirmed = true` only then. This root
   does not create or assign an account-level metastore.
5. Before the Phase 1 catalog/grants can be created, provide the required,
   non-versioned name of one approved existing account group and set
   `approved_engineer_writer_group_confirmed = true`. No group is created.
   The root creates catalog `tt_ecommerce_synthetic` by default and empty
   `bronze`, `silver`, `quarantine`, and `gold` schemas **without**
   `storage_root`; their managed tables therefore require the approved metastore
   DEFAULT location. Do not substitute the `lakehouse` external location as a
   managed-storage root. If the metastore has no approved DEFAULT location, do
   not apply this root: obtain that decision/configuration from the UC admin
   first.

The Phase 1 IaC declares only `USE_CATALOG` plus `USE_SCHEMA` and
`CREATE_TABLE` on each schema for the writer group, through principal-scoped
`databricks_grant` resources. It declares no analyst, BI, steward,
`account users`, `SELECT`, `ALL_PRIVILEGES`, metastore-level, or default
`MODIFY` grant. This is not proof of exclusive effective access: these grants
do not revoke inherited/pre-existing grants, and catalog/schema/location owners
and table creators may have broader powers (including table read/modify).
Before apply, inventory existing metastore, credential, and external-location
grants/owners; approve a dedicated applying/owner identity and exact group
membership. After authorized apply, verify owners and effective permitted and
denied access on the new catalog/four schemas and credential/location, with no
Phase 1 data. No mask, PII, group, or `force_destroy` is configured.

The platform root creates `landing` and `lakehouse` filesystems and empty
directory markers for the Phase 0 sources and Bronze/Silver/quarantine/Gold
layout. Actual ingestion paths, tables, jobs, clusters, and DBU compute are
intentionally out of scope. The optional SQL Warehouse is off by default and
is Phase 5 only: setting `enable_sql_warehouse = true` also requires the
separate `phase5_sql_warehouse_approved = true` confirmation after Gold,
exact `PRO` / `2X-Small` pricing, permission, quota, and cost review. Its cost
must not be substituted with a serverless estimate. A budget alert is not a
spending cap and is omitted until an approved notification contact exists. None
of these statements claim resources or protections are deployed.

Do not use `terraform destroy` on `platform`, or `-target`, to stop compute: this root also owns the resource group, ADLS data paths, workspace, Access Connector, and its RBAC assignment. `bootstrap` state storage is a separate root and must not be treated as session-cleanup infrastructure.

On 2026-11-03, only the `databricks` root was checked offline with temporary
Terraform 1.13.5 verified against the official SHA256 and provider 1.80.0:
`fmt -check -recursive`, `init -backend=false -lockfile=readonly`, and
`validate` all exited 0; `git diff --check` was clean. No credentials, Azure,
state, or plan were used. Other roots were not validated in that run.

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
