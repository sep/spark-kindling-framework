# =============================================================================
# Workspace Configuration
# =============================================================================

resource "databricks_workspace_conf" "this" {
  count = length(var.workspace_conf) > 0 ? 1 : 0

  custom_config = var.workspace_conf
}
