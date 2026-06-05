# Entity Configuration via YAML

Entity tags can be configured via standard YAML configuration files using the `entity_tags` key. These tags are merged at retrieval time, so configuration can be loaded before or after entity registration - order doesn't matter.

## Configuration Format

In your YAML configuration file (e.g., `settings.yaml`, `production.yaml`), add entity tag overrides under the `entity_tags` namespace:

```yaml
entity_tags:
  bronze.orders:
    provider.path: "abfss://production@storage.dfs.core.windows.net/bronze/orders"
    environment: "production"
    retention_days: "90"

  bronze.customers:
    provider.path: "abfss://production@storage.dfs.core.windows.net/bronze/customers"
    environment: "production"

  ref.categories:
    provider.path: "Files/reference/categories.csv"
    provider.header: "true"
    provider.inferSchema: "true"
```

## How It Works

1. **Entity Registration (Code)**: Entities are registered in Python code with base tags:
   ```python
   @DataEntities.entity(
       entityid="bronze.orders",
       name="orders",
       partition_columns=["date"],
       merge_columns=["order_id"],
       tags={
           "provider.type": "delta",
           "layer": "bronze"
       },
       schema=None
   )
   class Orders:
       pass
   ```

2. **Configuration Override (YAML)**: Environment-specific tags are defined in config:
   ```yaml
   entity_tags:
     bronze.orders:
       provider.path: "abfss://production@storage/bronze/orders"
       environment: "production"
   ```

3. **Retrieval-Time Merging**: When you get the entity, tags are merged automatically:
   ```python
   entity = data_entity_manager.get_entity_definition("bronze.orders")
   # entity.tags will contain:
   # {
   #   "provider.type": "delta",       # From code (base)
   #   "layer": "bronze",               # From code (base)
   #   "provider.path": "abfss://...",  # From config (override)
   #   "environment": "production"      # From config (override)
   # }
   ```

## Benefits

- **Order Independent**: Configuration can be loaded before or after entity registration
- **Environment-Specific**: Different configurations per environment (dev, staging, production)
- **Centralized**: All configuration in standard YAML files
- **No Code Changes**: Change paths and settings without touching Python code

## Example: Multi-Environment Configuration

**development.yaml:**
```yaml
entity_tags:
  bronze.orders:
    provider.path: "Files/dev/bronze/orders"
    environment: "development"
  bronze.customers:
    provider.path: "Files/dev/bronze/customers"
    environment: "development"
```

**production.yaml:**
```yaml
entity_tags:
  bronze.orders:
    provider.path: "abfss://production@storage.dfs.core.windows.net/bronze/orders"
    environment: "production"
    retention_days: "90"
  bronze.customers:
    provider.path: "abfss://production@storage.dfs.core.windows.net/bronze/customers"
    environment: "production"
    retention_days: "365"
```

## Configuration Loading

Entity tag configuration is loaded automatically through the standard Kindling configuration system:

```python
bootstrap_framework(
    config_files=["settings.yaml", "production.yaml"],
    environment="production"
)
```

The framework will merge entity tags from all config files, with later files overriding earlier ones.
