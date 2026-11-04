# Terraform infrastructure lanes

Each directory below is an independent Terraform root. `bootstrap` retains its
local state; the user reports `platform` has its AzureRM backend configured.
No root creates its own state backend. Do not commit
`terraform.tfstate`, provider credentials, or a real `.tfvars` file.

| Root | Purpose | Default scope |
| --- | --- | --- |
| `bootstrap` | Optional, future Azure Blob state storage | Not consumed automatically by other roots |
| `platform` | Resource group, ADLS Gen2 storage, lake paths, Databricks Premium workspace, and connector identity | Base deployment |
| `databricks` | Phase 1 workspace-level UC credential/external location, synthetic catalog, Bronze/Silver/quarantine/Gold schemas, and minimal writer grants; optional Phase 5 SQL Warehouse | Explicitly gated; SQL is off by default |

The planned region for `platform` and optional `bootstrap` is **France Central**. A user-observed West Europe portal warning is not evidence that France Central supports this subscription, Free Trial quota, or Databricks Premium. The user reports Stages 1–3 of `platform` applied: seven paths plus private managed-storage infrastructure with its dedicated connector and container-scoped grant. Approved UC design leaves the existing metastore DEFAULT root empty: the synthetic catalog uses a catalog-level managed-storage root, while `lakehouse` remains isolated for external location. No plan may mutate or replace the metastore. The user also reports the cloud-only identity is Account Admin, Workspace Admin and sole Metastore Admin after an authorized transfer; it has no reported Azure subscription roles. Databricks CLI v1.22.1 is installed and a local workspace OAuth profile authenticated `current-user me` with exit 0. A separate cloud-only non-admin engineer identity, an engineering account group, and its membership were created; the group has Workspace access only, with no consumer/SQL/admin, unrestricted clusters, or pools. User-reported final checks confirm the UC objects/grants via direct read-only calls, permitted catalog/schema reads and denied external-location/credential access for the restricted writer identity. The final post-removal Terraform plan converged with 0 add/change/destroy, while the administrative Databricks profile remained operational. A monthly 50 EUR subscription budget with alerts is reported created; forecast charges cited by the user belong to other subscription projects. Inventory reports 0 clusters, 0 jobs, and one pre-existing stopped SQL Warehouse; Terraform keeps SQL Warehouse disabled and did not create it. No synthetic project data, jobs, compute, or started SQL Warehouse exist. Fase 1 is closed according to user-reported evidence; later data, job, compute, and SQL/BI phases remain separate. This is user-reported, not independent cloud evidence. Do not use cleanup, `destroy`, or `-target`. For the approved future-session lifecycle (keep `platform`; stop active compute rather than destroy infrastructure), see [Fase 1 development cycle](../docs/fase-1/CICLO_DE_DESARROLLO.md). Phase 1 gates are closed; future phases require their own gates.

## Safe deployment order

1. Agree France Central, the tenant, subscription, naming prefix, tags, and a network pattern; recheck regional quota/Premium eligibility and persistent workspace/ADLS cost, budget, alerts, and owner. Do not use Portal **Create** instead of the reviewed Terraform roots.
   AzureRM 4.30.0 will use `features {}` and `resource_provider_registrations = "none"` in `platform`/`bootstrap`: it will not auto-register providers. A plan needs Azure CLI Owner access and an `ARM_SUBSCRIPTION_ID` kept only in the current local shell; never commit, persist, or share it. Before the separately authorized bootstrap plan, inspect (read-only) `Microsoft.Storage`, `Microsoft.Authorization`, and `Microsoft.Databricks` registration states. A user reports the last as Registered; confirm it and pause if a required provider is not Registered rather than using Portal **Register**.
   This code keeps public endpoints enabled on the workspace and storage for the
   synthetic-data demo. This Phase 1 G5 posture is approved and recorded: they are reachable from any network, subject to
   authentication/RBAC, with no network allowlist. Anonymous blob access is denied.
   TLS, RBAC, and this synthetic-only posture make no private-network, live-verification,
   or GDPR-compliance claim. Disabling workspace public access requires
   a Private Link/VNet design; do not simply flip that setting without
   implementing the private network. Storage public access likewise needs an
   approved private endpoint/DNS and deployer-access design before it is
   disabled.
2. Optionally deploy `bootstrap` and have platform/security assign least
   privilege to the state storage. The user reports two successful bootstrap
   applies: RG, Standard LRS Storage Account, and private `tfstate` container;
   then the Terraform-managed **Storage Blob Data Contributor** assignment at
   that container scope for the current Azure CLI identity. Bootstrap state is
   still local. A prior platform init found no `backend "azurerm" {}` and
   therefore remained local; the user reports the platform backend was later
   configured before Stage 1. This historic warning does not imply a remaining
   backend action or broader data-plane access.
   No Portal role assignment is needed unless approved recovery requires it.
   Subscription Owner alone does not grant Entra Blob data-plane access. The
   state account uses authenticated public access without allowlist,
   OAuth by default, no Shared Key/local users, and 7-day blob versioning plus
   blob/container soft delete. `tfstate` is sensitive (resource IDs and possibly
   sensitive values), not PII by design, and not public because its endpoint is
   public; this is not a private-network or GDPR claim. See
   `bootstrap/backend.hcl.example`.
3. The user reports Stage 1 of `platform` applied nine expected resources: the
   foundation, cost-bearing workspace, HNS storage, private `landing` and
   `lakehouse` containers, Access Connector, Connector Blob Data Contributor
   limited to `lakehouse`, and the current CLI operator's Blob Data Contributor
   grants on each container. The user reports separately reviewed/authorized
   Stage 2 created exactly seven Gen2 paths, with no changes or destroys. Both
   `create_data_lake_directories` and
   `terraform_operator_blob_access_confirmed` remain `true` and must never be
   reset to `false`, because that would destroy the paths. Do not use `-target`;
   every future plan/apply still requires separate review and authorization.
   Operator grants remain distinct from the Connector and grant no
   future-container access. Shared keys and anonymous blob access stay disabled.
4. Before using `databricks`, an administrator must confirm an existing Unity
   Catalog metastore is assigned to the workspace, the applying identity has the
   required workspace/UC role and object-owner authority, and the metastore has
    an approved parent external location and catalog child path for managed
    storage. Set
   `unity_catalog_prerequisites_confirmed = true` and
   `metastore_default_managed_storage_confirmed = true` only then. This root
   does not create or assign an account-level metastore. The user reports its
    DEFAULT root is intentionally left empty, while Stage 3 provides dedicated
   private managed storage, connector, and container-scoped grant. Add the
   approved identity to the workspace and verify workspace/metastore authority.
   Then configure the approved parent external location and catalog child path
   with identity/credential; never mutate or replace the metastore. `lakehouse`
   remains reserved for the intended external location.
   The user reports the cloud-only identity is now the account/workspace/sole
   metastore administrator, but its Terraform authentication must still be
   verified. Create/approve a least-privilege real writer group and confirm
   effective membership before setting gates, validating, reviewing a plan, and
   separately authorizing an apply. Retain temporary Global Administrator until
   cloud/provider access, effective grants, and recovery criteria are verified.
5. Before the Phase 1 catalog/grants can be created, provide the required,
   non-versioned name of one approved existing account group and set
   `approved_engineer_writer_group_confirmed = true`. No group is created.
   The root creates catalog `tt_ecommerce_synthetic` by default and empty
    `bronze`, `silver`, `quarantine`, and `gold` schemas with the approved
    catalog child path for managed storage. Do not substitute the `lakehouse`
    external location as a managed-storage root. If the parent external location
    or child path has not been approved, do not apply this root: obtain that
    decision/configuration from the UC admin first.

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

The user reports Stage 2 created the seven directory markers for the Phase 0
sources and Bronze/Silver/quarantine/Gold layout: `landing/orders`,
`landing/customers`, `landing/products`, `lakehouse/bronze`,
`lakehouse/silver`, `lakehouse/quarantine`, and `lakehouse/gold`. Their two
creation/confirmation flags remain `true`; do not reset either to `false`.
Actual ingestion paths, tables, jobs, clusters, and DBU compute are
intentionally out of scope. The optional SQL Warehouse is off by default and
is Phase 5 only: setting `enable_sql_warehouse = true` also requires the
separate `phase5_sql_warehouse_approved = true` confirmation after Gold,
exact `PRO` / `2X-Small` pricing, permission, quota, and cost review. Its cost
must not be substituted with a serverless estimate. A budget alert is not a
spending cap and is omitted until an approved notification contact exists. None
of these statements claim resources or protections are deployed.

Do not use `terraform destroy` on `platform`, or `-target`, to stop compute: this root also owns the resource group, ADLS data paths, workspace, Access Connector, and its RBAC assignment. `bootstrap` state storage is a separate root and must not be treated as session-cleanup infrastructure.

On 2026-11-03, the user checked all roots offline with temporary verified
Terraform 1.13.5 Windows amd64. `platform` and `bootstrap` used signed
azurerm 4.30.0; `databricks` used provider 1.80.0. All completed fmt/init with
`-backend=false -lockfile=readonly`/validate with exit 0. Platform initially
reported a deprecated `resource_manager_id`; its replacement with the
`lakehouse` container ID preserved container scope, and final validate had no
warning. `git diff --check` was clean; `.terraform` remained ignored and lock
files unchanged. No credentials, Azure auth/API, state, plan, apply, or destroy
were used. This is local code evidence only; inspect France Central
quota/eligibility and offer/cost/budget in Portal before any separately approved
plan.

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
