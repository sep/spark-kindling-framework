# Google Cloud Dataproc Platform Evaluation

## Executive Summary

This document evaluates adding Google Cloud Dataproc as a fourth platform to the Kindling framework, alongside Microsoft Fabric, Azure Synapse Analytics, and Databricks. The evaluation covers API compatibility, implementation complexity, and strategic considerations.

**Recommendation: ✅ FAVORABLE** - Dataproc is a strong candidate for inclusion with manageable implementation effort.

---

## 1. Platform Overview

### What is Google Dataproc?

Google Cloud Dataproc is a fully managed Apache Spark and Apache Hadoop service that makes it easy to run big data workloads on Google Cloud. Key characteristics:

| Feature | Dataproc | Comparison to Existing Platforms |
|---------|----------|----------------------------------|
| **Spark Support** | Native Apache Spark | Same as all current platforms |
| **Deployment Model** | Cluster-based + Serverless | Similar to Synapse (pools) |
| **Storage** | Google Cloud Storage (GCS) | Analogous to ADLS/ABFSS |
| **Authentication** | Google Cloud IAM + ADC | Similar pattern to Azure AD |
| **Python SDK** | `google-cloud-dataproc` | Well-maintained, official SDK |
| **REST API** | Full REST API v1 | Mature, well-documented |
| **Delta Lake** | Supported via connectors | Same as other platforms |

### Dataproc Variants

1. **Dataproc on Compute Engine** - Traditional cluster-based (VMs)
2. **Dataproc Serverless** - Serverless Spark execution
3. **Dataproc on GKE** - Kubernetes-based deployment

**Initial Target**: Dataproc on Compute Engine (most similar to existing platforms)

---

## 2. API Compatibility Analysis

### PlatformAPI Method Mapping

| Kindling PlatformAPI | Dataproc Equivalent | Implementation Notes |
|---------------------|---------------------|---------------------|
| `get_platform_name()` | N/A | Return `"dataproc"` |
| `create_spark_job()` | `projects.regions.jobs.submit` | Direct mapping |
| `upload_files()` | GCS Client Library | Upload to `gs://` bucket |
| `update_job_files()` | N/A (included in submit) | Files referenced at submit time |
| `run_job()` | `jobs.submit` or `submitAsOperation` | Submit triggers execution |
| `get_job_status()` | `jobs.get` | Maps to Job.status.state |
| `cancel_job()` | `jobs.cancel` | Direct mapping |
| `delete_job()` | `jobs.delete` | Direct mapping |
| `get_job_logs()` | `driverOutputResourceUri` + GCS | Logs stored in GCS |
| `deploy_spark_job()` | Composite operation | Upload + Submit |

### Key Differences from Existing Platforms

#### 1. Job Submission Model

**Existing Platforms (Fabric/Synapse/Databricks)**:
```
1. Create job definition → returns job_id
2. Upload files to storage
3. Update job with file paths
4. Run job → returns run_id
```

**Dataproc**:
```
1. Upload files to GCS
2. Submit job (includes config + file references) → returns job_id
   Job starts immediately upon submission
```

**Impact**: Dataproc combines "create" and "run" into a single operation. The `create_spark_job` method would need to be a no-op or return a pending configuration, with actual job creation happening in `run_job`.

#### 2. Storage Paths

| Platform | Storage URI Format |
|----------|-------------------|
| Fabric | `abfss://container@account.dfs.core.windows.net/path` |
| Synapse | `abfss://container@account.dfs.core.windows.net/path` |
| Databricks | `dbfs:/path` or `abfss://...` |
| **Dataproc** | `gs://bucket-name/path` |

**Impact**: Storage path handling needs GCS-specific logic.

#### 3. Job State Machine

```
Dataproc States:
PENDING → SETUP_DONE → RUNNING → DONE/ERROR/CANCELLED

Kindling Normalized States:
PENDING → RUNNING → COMPLETED/FAILED/CANCELLED
```

**State Mapping**:
```python
DATAPROC_STATUS_MAP = {
    "PENDING": "PENDING",
    "SETUP_DONE": "PENDING",
    "RUNNING": "RUNNING",
    "CANCEL_PENDING": "CANCELLING",
    "CANCEL_STARTED": "CANCELLING",
    "CANCELLED": "CANCELLED",
    "DONE": "COMPLETED",
    "ERROR": "FAILED",
    "ATTEMPT_FAILURE": "FAILED",
}
```

#### 4. Log Retrieval

Dataproc provides `driverOutputResourceUri` pointing to GCS location:
```
gs://bucket/google-cloud-dataproc-metainfo/{uuid}/jobs/{job_id}/driveroutput
```

**Implementation**: Use GCS client to read log files from this path.

---

## 3. Implementation Design

### Class Structure

```python
# packages/kindling/platform_dataproc.py

from google.cloud import dataproc_v1
from google.cloud import storage
from kindling.platform_provider import PlatformAPI

class DataprocService(PlatformAPI):
    """Google Cloud Dataproc platform implementation"""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.project_id = config.get("project_id")
        self.region = config.get("region", "us-central1")
        self.cluster_name = config.get("cluster_name")
        self.gcs_bucket = config.get("gcs_bucket")

        # Initialize clients
        self._job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )
        self._storage_client = storage.Client(project=self.project_id)

        # Pending job config (held until run_job is called)
        self._pending_jobs = {}
```

### Method Implementations

#### Core Job Methods

```python
def get_platform_name(self) -> str:
    return "dataproc"

def create_spark_job(self, job_name: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare job configuration (actual submission happens in run_job)"""
    job_id = f"{job_name}-{int(time.time())}"

    # Store configuration for later submission
    self._pending_jobs[job_id] = {
        "job_name": job_name,
        "config": job_config,
        "files_path": None,
    }

    return {
        "job_id": job_id,
        "status": "PENDING_SUBMISSION",
        "job_name": job_name,
    }

def upload_files(self, files: Dict[str, str], target_path: str) -> str:
    """Upload files to Google Cloud Storage"""
    bucket = self._storage_client.bucket(self.gcs_bucket)

    for filename, content in files.items():
        blob_path = f"{target_path.strip('/')}/{filename}"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content)
        self.logger.debug(f"Uploaded {filename} to gs://{self.gcs_bucket}/{blob_path}")

    return f"gs://{self.gcs_bucket}/{target_path.strip('/')}"

def update_job_files(self, job_id: str, files_path: str) -> None:
    """Associate uploaded files with pending job"""
    if job_id in self._pending_jobs:
        self._pending_jobs[job_id]["files_path"] = files_path

def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
    """Submit job to Dataproc cluster"""
    pending = self._pending_jobs.get(job_id)
    if not pending:
        raise ValueError(f"No pending job found for {job_id}")

    files_path = pending["files_path"]
    config = pending["config"]

    # Build PySpark job specification
    main_file = config.get("main_file", "kindling_bootstrap.py")
    job = {
        "placement": {"cluster_name": self.cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"{files_path}/{main_file}",
            "args": self._build_bootstrap_args(config),
            "python_file_uris": [f"{files_path}/*.py"],
        },
        "reference": {
            "project_id": self.project_id,
            "job_id": job_id,
        },
    }

    # Submit job
    operation = self._job_client.submit_job_as_operation(
        request={
            "project_id": self.project_id,
            "region": self.region,
            "job": job,
        }
    )

    # Wait for submission to complete
    result = operation.result()

    # Clean up pending job
    del self._pending_jobs[job_id]

    return result.reference.job_id

def get_job_status(self, run_id: str) -> Dict[str, Any]:
    """Get job execution status"""
    job = self._job_client.get_job(
        project_id=self.project_id,
        region=self.region,
        job_id=run_id,
    )

    status = job.status.state.name

    return {
        "status": DATAPROC_STATUS_MAP.get(status, status),
        "start_time": job.status.state_start_time.isoformat() if job.status.state_start_time else None,
        "end_time": None,  # Derived from final state time
        "error": job.status.details if status == "ERROR" else None,
        "driver_output_uri": job.driver_output_resource_uri,
    }

def cancel_job(self, run_id: str) -> bool:
    """Cancel a running job"""
    try:
        self._job_client.cancel_job(
            project_id=self.project_id,
            region=self.region,
            job_id=run_id,
        )
        return True
    except Exception as e:
        self.logger.error(f"Failed to cancel job {run_id}: {e}")
        return False

def delete_job(self, job_id: str) -> bool:
    """Delete a job from Dataproc"""
    try:
        self._job_client.delete_job(
            project_id=self.project_id,
            region=self.region,
            job_id=job_id,
        )
        return True
    except Exception as e:
        self.logger.error(f"Failed to delete job {job_id}: {e}")
        return False

def get_job_logs(self, run_id: str, from_line: int = 0, size: int = 1000) -> Dict[str, Any]:
    """Get job logs from GCS"""
    job = self._job_client.get_job(
        project_id=self.project_id,
        region=self.region,
        job_id=run_id,
    )

    driver_output_uri = job.driver_output_resource_uri
    if not driver_output_uri:
        return {"log": [], "total_lines": 0, "has_more": False}

    # Parse GCS URI: gs://bucket/path
    # Download and return log content
    bucket_name, blob_path = self._parse_gcs_uri(driver_output_uri)
    bucket = self._storage_client.bucket(bucket_name)

    # Driver output has numbered suffixes (.000000000, .000000001, etc.)
    logs = []
    blob = bucket.blob(f"{blob_path}.000000000")
    if blob.exists():
        content = blob.download_as_text()
        lines = content.split('\n')
        logs = lines[from_line:from_line + size]

    return {
        "log": logs,
        "total_lines": len(lines) if lines else 0,
        "from_line": from_line,
        "has_more": (from_line + size) < len(lines) if lines else False,
    }
```

### Storage Utilities Integration

For notebook runtime operations, a companion class would handle GCS operations:

```python
class DataprocStorageUtils:
    """GCS storage operations for Dataproc notebook runtime"""

    def __init__(self, spark_session):
        self.spark = spark_session

    def exists(self, path: str) -> bool:
        """Check if GCS path exists"""
        from google.cloud import storage
        bucket_name, blob_path = self._parse_gcs_uri(path)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        return bucket.blob(blob_path).exists()

    def read(self, path: str) -> str:
        """Read file from GCS"""
        from google.cloud import storage
        bucket_name, blob_path = self._parse_gcs_uri(path)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        return bucket.blob(blob_path).download_as_text()

    def write(self, path: str, content: str) -> None:
        """Write file to GCS"""
        from google.cloud import storage
        bucket_name, blob_path = self._parse_gcs_uri(path)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        bucket.blob(blob_path).upload_from_string(content)

    def copy(self, source: str, destination: str) -> None:
        """Copy file in GCS"""
        from google.cloud import storage
        client = storage.Client()

        src_bucket, src_path = self._parse_gcs_uri(source)
        dst_bucket, dst_path = self._parse_gcs_uri(destination)

        source_bucket = client.bucket(src_bucket)
        source_blob = source_bucket.blob(src_path)
        destination_bucket = client.bucket(dst_bucket)

        source_bucket.copy_blob(source_blob, destination_bucket, dst_path)
```

---

## 4. Dependencies

### Required Python Packages

```toml
# pyproject.toml additions
[tool.poetry.dependencies]
google-cloud-dataproc = ">=5.0.0"
google-cloud-storage = ">=2.10.0"
google-auth = ">=2.20.0"
```

### Package Size Impact

| Package | Size | Current in Kindling |
|---------|------|---------------------|
| google-cloud-dataproc | ~1.5 MB | No |
| google-cloud-storage | ~500 KB | No |
| google-auth | ~300 KB | No |
| **Total** | ~2.3 MB | Azure SDK ~5 MB |

**Assessment**: Reasonable addition, smaller than existing Azure dependencies.

---

## 5. Authentication Strategy

### Supported Methods (Priority Order)

1. **Application Default Credentials (ADC)**
   ```python
   # Automatically uses:
   # - GOOGLE_APPLICATION_CREDENTIALS env var
   # - gcloud auth application-default login
   # - GCE/GKE service account
   from google.auth import default
   credentials, project = default()
   ```

2. **Service Account JSON Key**
   ```python
   from google.oauth2 import service_account
   credentials = service_account.Credentials.from_service_account_file(
       'path/to/service-account.json'
   )
   ```

3. **Workload Identity (GKE)**
   ```python
   # Automatic when running on GKE with Workload Identity configured
   ```

### Implementation

```python
def _get_credentials(self):
    """Get Google Cloud credentials with fallback chain"""
    import os
    from google.auth import default
    from google.oauth2 import service_account

    # Check for explicit service account file
    sa_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_file and os.path.exists(sa_file):
        return service_account.Credentials.from_service_account_file(sa_file)

    # Fall back to ADC
    credentials, project = default()
    return credentials
```

---

## 6. Configuration Schema

### Environment Variables

```bash
# Required
DATAPROC_PROJECT_ID=my-gcp-project
DATAPROC_REGION=us-central1
DATAPROC_CLUSTER_NAME=my-spark-cluster
DATAPROC_GCS_BUCKET=my-kindling-artifacts

# Optional
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### Kindling Config (settings.yaml)

```yaml
kindling:
  platform:
    name: dataproc

  dataproc:
    project_id: ${DATAPROC_PROJECT_ID}
    region: ${DATAPROC_REGION:us-central1}
    cluster_name: ${DATAPROC_CLUSTER_NAME}
    gcs_bucket: ${DATAPROC_GCS_BUCKET}

    # Optional cluster configuration for ephemeral clusters
    cluster_config:
      master_machine_type: n1-standard-4
      worker_machine_type: n1-standard-4
      num_workers: 2
      spark_version: "3.4"
```

---

## 7. Implementation Roadmap

### Phase 1: Core Platform API (2-3 weeks)

| Task | Effort | Priority |
|------|--------|----------|
| Create `platform_dataproc.py` skeleton | 1 day | P0 |
| Implement `DataprocService` class | 3 days | P0 |
| Implement job lifecycle methods | 2 days | P0 |
| Implement storage operations | 2 days | P0 |
| Add GCS-based log retrieval | 1 day | P0 |
| Unit tests | 2 days | P0 |

### Phase 2: Bootstrap Integration (1 week)

| Task | Effort | Priority |
|------|--------|----------|
| Adapt bootstrap script for Dataproc | 2 days | P0 |
| Handle GCS paths in bootstrap | 1 day | P0 |
| Platform detection logic | 1 day | P0 |
| Integration tests | 1 day | P0 |

### Phase 3: System Tests & CI/CD (1-2 weeks)

| Task | Effort | Priority |
|------|--------|----------|
| Create Dataproc test infrastructure | 3 days | P1 |
| Write system tests | 2 days | P1 |
| Add CI/CD workflow for Dataproc | 2 days | P1 |
| Build `kindling_dataproc` wheel | 1 day | P1 |
| Documentation | 2 days | P1 |

### Phase 4: Advanced Features (Optional)

| Task | Effort | Priority |
|------|--------|----------|
| Dataproc Serverless support | 1 week | P2 |
| Ephemeral cluster management | 1 week | P2 |
| BigQuery integration | 3 days | P2 |
| Dataproc Metastore support | 2 days | P2 |

**Total Estimated Effort**: 5-7 weeks for full production readiness

---

## 8. Risk Assessment

### Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Job model mismatch (create vs submit) | Medium | Medium | Adapter pattern with pending jobs |
| Log format differences | Low | Low | Flexible log parser |
| Auth complexity in notebooks | Medium | Medium | ADC provides seamless fallback |
| Delta Lake compatibility | Low | Medium | Use delta-spark connector |

### Operational Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Increased test infrastructure cost | High | Low | Use preemptible VMs |
| GCP expertise requirement | Medium | Medium | Comprehensive documentation |
| Dependency version conflicts | Low | Medium | Careful version pinning |

---

## 9. Strategic Considerations

### Benefits

1. **Market Expansion** - Access to GCP-centric organizations
2. **Multi-Cloud Support** - True cloud-agnostic framework
3. **Feature Parity** - Dataproc has strong Spark support
4. **Cost Optimization** - Preemptible VMs can reduce costs

### Challenges

1. **Maintenance Burden** - Fourth platform to maintain
2. **Testing Complexity** - Need GCP test infrastructure
3. **Documentation** - Additional platform-specific docs
4. **Support Scope** - More platforms = more support requests

### Recommendation

**Proceed with Implementation** based on:

1. ✅ Clear API mapping to existing PlatformAPI interface
2. ✅ Mature, well-documented Python SDK
3. ✅ Similar authentication patterns to existing Azure platforms
4. ✅ Manageable implementation effort (5-7 weeks)
5. ✅ Strategic value for multi-cloud customers

---

## 10. Appendix: API Reference Comparison

### Job Status States

| Dataproc | Fabric | Synapse | Databricks | Kindling Normalized |
|----------|--------|---------|------------|---------------------|
| PENDING | Pending | NotStarted | PENDING | PENDING |
| SETUP_DONE | - | - | - | PENDING |
| RUNNING | Running | Running | RUNNING | RUNNING |
| DONE | Completed | Succeeded | TERMINATED (success) | COMPLETED |
| ERROR | Failed | Error | TERMINATED (failure) | FAILED |
| CANCELLED | Cancelled | Cancelled | TERMINATED (cancelled) | CANCELLED |

### Storage Path Formats

| Platform | Scheme | Example |
|----------|--------|---------|
| Fabric | abfss:// | `abfss://container@storage.dfs.core.windows.net/path` |
| Synapse | abfss:// | `abfss://container@storage.dfs.core.windows.net/path` |
| Databricks | dbfs:// or abfss:// | `dbfs:/mnt/data` or `abfss://...` |
| Dataproc | gs:// | `gs://bucket-name/path/to/file` |

### Python SDK Comparison

| Operation | Fabric | Synapse | Databricks | Dataproc |
|-----------|--------|---------|------------|----------|
| SDK Package | azure-synapse-artifacts | azure-synapse-artifacts | databricks-sdk | google-cloud-dataproc |
| Auth Package | azure-identity | azure-identity | databricks-sdk | google-auth |
| Storage Package | azure-storage-file-datalake | azure-storage-file-datalake | databricks-sdk | google-cloud-storage |

---

## 11. Next Steps

1. **Approval** - Review this evaluation with stakeholders
2. **Infrastructure Setup** - Provision GCP test environment
3. **Implementation** - Begin Phase 1 development
4. **Documentation** - Create Dataproc-specific user guides
5. **Release** - Include in next minor version (v0.5.0)

---

*Document Version: 1.0*
*Date: February 2, 2026*
*Author: Kindling Framework Team*
