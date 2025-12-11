# Injection Pattern Analysis: GlobalInjector.get() vs get_kindling_service()

**Date:** December 10, 2025
**Analysis Type:** Code Pattern Evaluation (No Changes)

---

## Executive Summary

**Question:** Should we adopt `get_kindling_service()` as the standard convention instead of `GlobalInjector.get()`?

**Answer:** **YES, with exceptions** - `get_kindling_service()` should be the preferred convention for user-facing code, but `GlobalInjector.get()` remains appropriate in specific contexts.

---

## Current State Analysis

### Pattern Distribution

**Total Occurrences Found:** ~50+ instances

#### 1. GlobalInjector.get() Usage

**Framework Internal Code (7 instances):**
- `data_entities.py` line 86: `GlobalInjector.get(DataEntityRegistry)`
- `data_pipes.py` line 45: `GlobalInjector.get(DataPipesRegistry)`
- `file_ingestion.py` line 42: `GlobalInjector.get(FileIngestionRegistry)`
- `simple_stage_processor.py` line 19: `GlobalInjector.get(StageProcessingService).execute()`
- `delta_entity_provider.py` line 437: `GlobalInjector.get(EntityPathLocator)`
- `spark_config.py` line 242: `GlobalInjector.get(ConfigService)`
- `injection.py` line 90: Implementation of `get_kindling_service()`

**Documentation Examples (3 instances):**
- `README.md` line 65: `executer = GlobalInjector.get(DataPipesExecution)`
- `entity_providers.md` line 143: `entity_provider = GlobalInjector.get(EntityProvider)`
- `file_ingestion.md` line 104: `processor = GlobalInjector.get(FileIngestionProcessor)`

**Test Code (40+ instances):**
- `test_injection.py`: 28 instances (testing DI framework itself)
- `test_config_integration.py`: 5 instances
- `test_spark_config.py`: 2 instances
- `test_signaling.py`: 2 instances

#### 2. get_kindling_service() Usage

**Bootstrap/Runtime Code (10 instances):**
- `bootstrap.py`: 8 instances
  - Line 277: `get_kindling_service(PlatformServiceProvider)`
  - Line 325: `get_kindling_service(PlatformServiceProvider)`
  - Line 332: `get_kindling_service(NotebookManager)`
  - Line 362: `get_kindling_service(PlatformServiceProvider)`
  - Line 382, 394, 403: `get_kindling_service(ConfigService)`
  - Line 405: `get_kindling_service(PythonLoggerProvider)`
  - Line 438: `get_kindling_service(NotebookManager)`
  - Line 452: `get_kindling_service(DataAppRunner)`

**Test Framework (3 instances):**
- `test_framework.py` lines 100-102: Uses `get_kindling_service()` for NotebookManager, PlatformServiceProvider, ConfigService

**Data Apps (4 instances):**
- `universal-test-app/main.py`: 4 instances
  - Line 23: `get_kindling_service(ConfigService)`
  - Line 41: `get_kindling_service(SparkLoggerProvider)`
  - Line 47: `get_kindling_service(PlatformServiceProvider)`

---

## Pattern Characteristics

### GlobalInjector.get()

**Characteristics:**
- Direct access to the DI container
- Lower-level API
- More explicit about using dependency injection
- Framework implementation detail exposed

**Current Usage Patterns:**
1. **Framework internal lazy initialization** (decorators)
2. **Documentation examples** (user-facing)
3. **Test code** (testing DI itself)
4. **Legacy code** (predates get_kindling_service)

### get_kindling_service()

**Characteristics:**
- Wrapper around GlobalInjector.get()
- Kindling-branded API
- Higher-level abstraction
- Framework-specific naming

**Current Usage Patterns:**
1. **Bootstrap/initialization code**
2. **Production runtime code**
3. **Data apps** (user code)
4. **Test framework utilities**

**Implementation:**
```python
def get_kindling_service(iface):
    return GlobalInjector.get(iface)
```

---

## Evaluation: Should We Standardize on get_kindling_service()?

### ✅ ARGUMENTS FOR get_kindling_service()

#### 1. **Framework Branding & Identity**
- **Pro:** Makes it clear you're using Kindling framework services
- **Example:** `get_kindling_service(ConfigService)` vs `GlobalInjector.get(ConfigService)`
- **Impact:** Better user experience, clear framework boundary

#### 2. **Abstraction & Future-Proofing**
- **Pro:** Hides implementation detail (GlobalInjector)
- **Pro:** Easier to change DI implementation later
- **Pro:** Could add Kindling-specific behavior (logging, validation, etc.)
- **Impact:** Better maintainability

#### 3. **Consistency with Modern Patterns**
- **Pro:** Follows framework-specific API pattern (like Django's `get_user_model()`)
- **Pro:** Matches existing production code (bootstrap.py, data apps)
- **Impact:** More predictable for users

#### 4. **Documentation Clarity**
- **Pro:** Simpler to explain: "Use `get_kindling_service()` to get framework services"
- **Pro:** Users don't need to learn about GlobalInjector concept
- **Impact:** Lower learning curve

#### 5. **Semantic Meaning**
- **Pro:** "Get Kindling service" is domain-specific and meaningful
- **Pro:** "GlobalInjector" exposes generic DI pattern
- **Impact:** Code reads more naturally

### ❌ ARGUMENTS AGAINST Standardizing (Exceptions)

#### 1. **Framework Internal Decorators**
**Current Pattern:**
```python
# In data_pipes.py
@classmethod
def pipe(cls, **decorator_params):
    def decorator(func):
        if cls.dpregistry is None:
            cls.dpregistry = GlobalInjector.get(DataPipesRegistry)
```

**Concern:** Framework internals implementing lazy initialization
- These are NOT user code
- Performance: Direct call may be marginally faster
- Clarity: Shows this is internal framework mechanism

**Recommendation:** **Keep GlobalInjector.get() in decorators** - this is framework implementation

#### 2. **Testing DI Framework Itself**
**Current Pattern:**
```python
# In test_injection.py
def test_bind_and_get():
    GlobalInjector.bind(TestInterface, TestImplementation)
    instance = GlobalInjector.get(TestInterface)
```

**Concern:** Tests validating DI framework behavior
- Testing the GlobalInjector class directly
- Using wrapper would mask what's being tested

**Recommendation:** **Keep GlobalInjector.get() in DI tests** - testing the actual class

#### 3. **Advanced Users / Framework Extensions**
**Scenario:** Power users extending the framework

**Concern:** They may need direct GlobalInjector access for:
- Custom bindings
- Scope management
- Advanced DI patterns

**Recommendation:** **Document both patterns** - get_kindling_service for standard use, GlobalInjector for advanced

---

## Recommended Convention

### Standard Pattern (98% of cases)

**USE `get_kindling_service()` FOR:**
- ✅ User-facing documentation examples
- ✅ Quickstart guides
- ✅ Data apps and pipelines (user code)
- ✅ Bootstrap and initialization
- ✅ Production runtime code
- ✅ Integration tests (testing user workflows)

**Example:**
```python
# RECOMMENDED
from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService

config = get_kindling_service(ConfigService)
```

### Exception Pattern (2% of cases)

**USE `GlobalInjector.get()` FOR:**
- ✅ Framework internal decorators (lazy init in @entity, @pipe)
- ✅ Unit tests testing DI framework itself
- ✅ Advanced extension patterns (documented separately)

**Example:**
```python
# Framework internal - OK to use GlobalInjector
@classmethod
def pipe(cls, **decorator_params):
    if cls.dpregistry is None:
        cls.dpregistry = GlobalInjector.get(DataPipesRegistry)
```

---

## Migration Assessment

### Files Needing Updates

**HIGH PRIORITY (User-Facing Documentation):**
1. ✅ Already using `get_kindling_service()`:
   - `bootstrap.py` ✓
   - `test_framework.py` ✓
   - `tests/data-apps/universal-test-app/main.py` ✓

2. ❌ Should migrate to `get_kindling_service()`:
   - `README.md` line 65
   - `docs/entity_providers.md` line 143, 162
   - `docs/file_ingestion.md` line 104

**MEDIUM PRIORITY (Integration Tests):**
3. ⚠️ Consider migrating (tests acting as user):
   - `tests/integration/test_config_integration.py`

**LOW PRIORITY (Keep as-is):**
4. ✓ Framework internals - no change needed:
   - `data_entities.py`
   - `data_pipes.py`
   - `file_ingestion.py`
   - `simple_stage_processor.py`
   - `delta_entity_provider.py`
   - `spark_config.py`

5. ✓ DI framework tests - no change needed:
   - `tests/unit/test_injection.py`
   - `tests/unit/test_spark_config.py`
   - `tests/unit/test_signaling.py`

### Migration Impact

**Estimated Changes:**
- Documentation: ~5 files, ~8 instances
- Integration tests: ~1 file, ~5 instances
- **Total:** ~13 instances to change

**Risk Level:** **LOW**
- Both functions are identical (simple wrapper)
- No breaking changes
- Can be done incrementally
- Old code continues to work

---

## Implementation Details

### Current Implementation

```python
# packages/kindling/injection.py line 89-90
def get_kindling_service(iface):
    return GlobalInjector.get(iface)
```

**Analysis:**
- ✅ Simple wrapper - zero overhead
- ✅ Type hints could be added
- ✅ Could add validation/logging in future
- ✅ Already exported in main modules

### Potential Enhancements

```python
from typing import TypeVar, Type

T = TypeVar('T')

def get_kindling_service(service_type: Type[T]) -> T:
    """
    Get a Kindling framework service instance.

    This is the recommended way to access Kindling services in your code.

    Args:
        service_type: The service interface or class type

    Returns:
        Instance of the requested service

    Examples:
        >>> config = get_kindling_service(ConfigService)
        >>> logger = get_kindling_service(SparkLoggerProvider)
    """
    return GlobalInjector.get(service_type)
```

---

## Comparison with Other Frameworks

### Django
- **Pattern:** `from django.contrib.auth import get_user_model`
- **Parallel:** Framework-specific getter, not generic DI
- **Learning:** Users learn framework API, not underlying implementation

### Angular (TypeScript)
- **Pattern:** `constructor(private service: MyService)`
- **Parallel:** Framework handles injection, users don't call injector directly
- **Learning:** Declarative, not imperative

### Spring (Java)
- **Pattern:** `@Autowired` annotations
- **Parallel:** Framework-managed, users don't call ApplicationContext.getBean() directly (usually)
- **Learning:** Declarative dependency injection

### ASP.NET Core
- **Pattern:** Constructor injection, rarely `IServiceProvider.GetService<T>()`
- **Parallel:** Framework convention hides DI container
- **Learning:** Users work with framework APIs

**Kindling Parallel:**
- `get_kindling_service()` = Framework API (high level)
- `GlobalInjector.get()` = DI Container API (low level)

**Best Practice:** Most frameworks hide DI internals behind framework-specific APIs

---

## Recommendations Summary

### 1. Adopt Official Convention ✅

**Policy:** "`get_kindling_service()` is the standard way to access Kindling services"

**Document in:**
- README.md (update existing example)
- Contributing guide
- API documentation
- Tutorial/quickstart

### 2. Update Documentation ✅

**Priority Changes:**
- README.md quickstart example
- entity_providers.md examples
- file_ingestion.md examples

**Timeline:** 1-2 hours

### 3. Preserve Exceptions ✅

**Keep GlobalInjector.get() in:**
- Framework internal decorators (data_entities.py, data_pipes.py, etc.)
- DI framework unit tests
- Advanced usage documentation (separate section)

### 4. Add Type Hints ✅

**Enhancement:**
```python
def get_kindling_service(service_type: Type[T]) -> T:
    return GlobalInjector.get(service_type)
```

**Benefit:** Better IDE support, type checking

### 5. Documentation Structure ✅

**Create sections:**
- **Standard Usage** → get_kindling_service()
- **Advanced Usage** → GlobalInjector.get() for power users
- **Framework Internals** → GlobalInjector.get() in decorators (internal docs)

---

## Decision Matrix

| Scenario | Use get_kindling_service()? | Rationale |
|----------|----------------------------|-----------|
| User documentation example | ✅ YES | Primary API for users |
| Data app code | ✅ YES | User-facing code |
| Bootstrap/initialization | ✅ YES | Already uses it |
| Integration tests | ✅ YES | Simulates user patterns |
| Framework decorators (internal) | ❌ NO | Implementation detail |
| DI framework unit tests | ❌ NO | Testing GlobalInjector itself |
| Advanced extension guide | ⚠️ BOTH | Document both patterns |

---

## Conclusion

**RECOMMENDATION: Standardize on `get_kindling_service()` with documented exceptions**

**Benefits:**
1. ✅ Better user experience (framework-branded API)
2. ✅ Consistent with modern framework patterns
3. ✅ Future-proof (can enhance without breaking)
4. ✅ Clearer semantic meaning
5. ✅ Lower learning curve for users
6. ✅ Already used in production code (bootstrap, data apps)

**Implementation:**
- **Phase 1:** Update documentation (3 files, ~8 instances)
- **Phase 2:** Update integration tests (optional, low priority)
- **Phase 3:** Add type hints and enhanced docstring
- **Phase 4:** Document exception patterns for advanced users

**Timeline:** 2-4 hours total

**Risk:** Minimal - wrapper is identical, both patterns will continue to work

---

**Document Version:** 1.0
**Last Updated:** December 10, 2025
**Next Review:** After documentation updates
