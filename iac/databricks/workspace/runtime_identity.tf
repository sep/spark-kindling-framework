# =============================================================================
# Runtime Identity (Entra app + service principal)
# =============================================================================

resource "azuread_application" "runtime" {
  count = var.create_runtime_service_principal ? 1 : 0

  display_name = var.runtime_service_principal_display_name
}

resource "azuread_service_principal" "runtime" {
  count = var.create_runtime_service_principal ? 1 : 0

  client_id = azuread_application.runtime[0].client_id
}

locals {
  runtime_sp_enabled = var.create_runtime_service_principal || (
    var.runtime_service_principal_application_id != null &&
    var.runtime_service_principal_application_id != ""
    ) || (
    var.storage_credential_sp_application_id != null &&
    var.storage_credential_sp_application_id != ""
  )

  runtime_sp_application_id_effective = var.create_runtime_service_principal ? azuread_application.runtime[0].client_id : coalesce(var.runtime_service_principal_application_id, var.storage_credential_sp_application_id)

  configured_service_principals_by_key = {
    for sp in var.service_principals : "app:${sp.application_id}" => sp
  }

  runtime_sp_already_configured = (
    local.runtime_sp_application_id_effective != null &&
    contains([for sp in var.service_principals : sp.application_id], local.runtime_sp_application_id_effective)
  )

  runtime_service_principal_by_key = local.runtime_sp_enabled && !local.runtime_sp_already_configured ? {
    runtime = {
      display_name   = var.runtime_service_principal_display_name
      application_id = local.runtime_sp_application_id_effective
    }
  } : {}

  runtime_sp_workspace_key = local.runtime_sp_already_configured ? "app:${local.runtime_sp_application_id_effective}" : "runtime"

  # Keys are static and known at plan time, values may contain apply-time IDs.
  service_principals_effective_by_key = merge(
    local.configured_service_principals_by_key,
    local.runtime_service_principal_by_key,
  )
}
