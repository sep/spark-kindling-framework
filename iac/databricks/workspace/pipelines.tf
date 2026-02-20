# =============================================================================
# DLT Pipelines
# =============================================================================

resource "databricks_pipeline" "pipelines" {
  for_each = { for p in var.dlt_pipelines : p.name => p }

  name          = each.value.name
  catalog       = coalesce(each.value.catalog, local.medallion_catalog_name_effective)
  target        = each.value.target_schema
  development   = each.value.development
  continuous    = each.value.continuous
  photon        = each.value.photon
  channel       = "CURRENT"
  configuration = each.value.configuration

  # Notebook-based pipeline
  dynamic "library" {
    for_each = each.value.notebook_path != null ? [each.value.notebook_path] : []
    content {
      notebook {
        path = library.value
      }
    }
  }

  # Glob-based pipeline (file pattern)
  dynamic "library" {
    for_each = each.value.file_glob != null ? [each.value.file_glob] : []
    content {
      file {
        path = library.value
      }
    }
  }

  cluster {
    label        = "default"
    node_type_id = each.value.node_type_id
    num_workers  = each.value.num_workers
  }

  depends_on = [
    databricks_catalog.main,
    databricks_schema.schemas,
  ]
}
