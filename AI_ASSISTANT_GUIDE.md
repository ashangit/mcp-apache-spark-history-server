# AI Assistant Guide for Spark Investigation

## ⚡ DECISION TREE - Start Here

```
Has app_id/job_id?
├─ YES → Validate first (get_application() or get_job_definition())
│  ├─ Job is SLOW → get_job_bottlenecks() → Drill down by issue type
│  ├─ Job FAILED → get_spark_job_logs(status="error") → Check error type
│  ├─ Job WON'T START → get_operator_logs() or get_admission_logs()
│  ├─ Job STUCK/HANGING → get_admission_logs() + list_events()
│  └─ COMPARE runs → compare_job_performance(id1, id2)
│
└─ NO ID → list_applications() or list_yoshi_jobs() → Present options → STOP

After finding issue type:
├─ Error message? → Check Error Symptom Mapping (below) → Go to fix
└─ Performance? → Check Metric Thresholds (below) → Go to fix
```

**Investigation Flow:** VALIDATE → ANALYZE → PRESENT (never ask users to run tools)

---

## 🎯 Scenario → Tool Mapping

| User Intent | First Tool(s) | Follow-up |
|------------|---------------|-----------|
| "Job X is slow" | `get_application(app_id)` + `get_job_bottlenecks()` | Drill: slowest_stages/sql/executors |
| "Job X failed" | `get_job_definition(job_id)` + `get_spark_job_logs(status="error")` | Check OOM/serialization errors |
| "Compare 2 runs" | `compare_job_performance(id1, id2)` | +`compare_job_environments()` |
| "Won't start" | `get_operator_logs()` or `get_admission_logs()` | Check queue/resource issues |
| "No app_id given" | `list_applications()` with filters | Present options, STOP |

---

## 🩺 Error Symptom → Issue Mapping (Fast Diagnosis)

| Error Message / Symptom | Issue Type | Fix Link |
|-------------------------|------------|----------|
| `Container killed with code 137` | JVM heap OOM | [Container Killed](#container-killed-exit-code-137) |
| `OutOfMemoryError: GC overhead limit exceeded` | GC pressure | [GC Fix](#gc-pressure-10) |
| `OutOfMemoryError: Java heap space` | Memory exhausted | [OOM Fix](#oom--job-failures) |
| `NotSerializableException` / `KryoException` | Serialization | [Serialization Fix](#serialization-errors) |
| `Could not execute broadcast in 300 secs` | Broadcast too large | [Broadcast Fix](#broadcast-join-failures) |
| `No space left on device` | Disk full / shuffle | [Disk Space Fix](#no-space-left-on-device) |
| `Artifact not found` | Build/deploy issue | [Artifact Fix](#job-not-starting--artifact-not-found) |
| Job slow, high spill memory | Disk spilling | [Spill Fix](#disk-spill-100mb) |
| Job slow, many stages in DAG | Excessive shuffle | [Shuffle Fix](#high-shuffle--slow-stages) |
| Job stuck, no progress | Hanging / scheduling | [Stuck Job Fix](#job-stuck--hanging) |
| High task duration variance | Data skew | [Skew Fix](#task-skew-3x) |

---

## 📊 Metric Thresholds (Fast Diagnosis)

| Metric | Healthy | Warning | Critical | Action |
|--------|---------|---------|----------|--------|
| **GC Pressure** | <5% | 5-10% | >10% | [GC Fix](#gc-pressure-10) |
| **Disk Spill** | 0 MB | <100 MB | >100 MB | [Spill Fix](#disk-spill-100mb) |
| **Task Skew** (P95/P50) | <2x | 2-3x | >3x | [Skew Fix](#task-skew-3x) |
| **Executor Util** | >80% | 50-80% | <50% | Check parallelism |
| **Failed Tasks** | 0 | 1-5 | >5 | Check logs/OOM |

---

## 🔥 Issue Resolution Guide

### GC Pressure (>10%)

**Quick Fix Priority:**
1. `df.unpersist()` unused cached data
2. `--conf spark.memory.fraction=0.4` (reduce memory for storage)
3. `--conf spark.sql.shuffle.partitions=400` (more partitions)
4. Fix data skew (see [Task Skew](#task-skew-3x))

**Advanced:**
- Use serialized storage: `df.persist(StorageLevel.MEMORY_ONLY_SER)`
- Check Datadog APM Profiling for allocation hotspots
- Last resort: `--conf spark.executor.extraJavaOptions="-XX:+UseZGC"` (Java 11+)

---

### Disk Spill (>100MB)

**Root Cause:** Data exceeds executor memory (serialized ratio ~6x: 1.4GB disk = 8.8GB memory)

**Fix Priority:**
1. **Increase partitions** (FIRST) → `df.repartition(N)` where N > current
   - Tool: `get_stage_task_summary()` to identify spilling stages
   - Safe until >10,000 partitions
   - Limitation: Won't fix severe skew
   
2. **Manage output files** → `df.coalesce(N)` before write (reduces file count)

3. **Increase memory** (LAST) → `--conf spark.executor.memory=6g`
   - Trade-off: Higher cost, lower parallelism

4. **Check skew** → If P95/P50 > 3x, see [Task Skew](#task-skew-3x)

---

### Task Skew (>3x)

**Fix Priority:**
1. **Salting** for skewed keys:
   ```python
   df.withColumn("salt", (rand() * 10).cast("int"))
     .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))
   ```
2. **Broadcast joins** → `spark.broadcast(small_df)`
3. **Enable AQE** → `--conf spark.sql.adaptive.enabled=true`

---

### Serialization Errors

**Symptoms:** `NotSerializableException` / `KryoException` / "object not serializable"

**Error Trace Example:**
```
object not serializable (class: org.scalatest.Assertions$AssertionsHelper)
field (class: org.scalatest.FunSuite, name: assertionsHelper, ...)
```

**Root Cause:** `.map()` captures non-serializable objects (connections, configs, SparkContext, test helpers)

**Fix Priority:**

1. **Move function out of scope** (BEST) - relocate outside test class/non-serializable context

2. **Initialize connections inside map()** (COMMON FIX):
   ```scala
   // ❌ BAD
   val conn = new DatabaseConnection("addr")
   df.map(x => conn.query(...))
   
   // ✅ GOOD
   df.map(x => {
     val conn = new DatabaseConnection("addr")
     conn.query(...)
   })
   ```

3. **Singleton pattern** - pass credentials, initialize inside map()

4. **SerializableWritable wrapper** (last resort) - may not work for all objects

**Debug:** Read trace top-to-bottom, check for test frameworks in scope, use `get_spark_job_logs(status="error")`

---

### Container Killed (Exit Code 137)

**Symptoms:** `Container killed with code 137`

**Root Cause:** 
- JVM heap OOM → configured with `-XX:OnOutOfMemoryError='kill -9 %p'` → exit code 137 (128 + 9)
- Kubernetes killed container for exceeding memory limits (off-heap/overhead)

**Fix Priority:**
1. **Check profiler** → Datadog APM Profiling to find memory hotspots
2. **Increase executor memory** → `--conf spark.executor.memory=6g`
3. **Increase partitions** → Reduce memory per executor (won't fix skew)
4. **Increase memory overhead** → `--conf spark.executor.memoryOverhead=1024` (for off-heap issues)
5. **Check for custom RDD code** → May have excessive heap usage

---

### OOM / Job Failures

**Quick Actions:**
1. `get_spark_job_logs(status="error")` - check for OOM/exception
2. Look for:
   - `OutOfMemoryError` → Increase `spark.executor.memory` or partitions
   - `java.lang.OutOfMemoryError: GC overhead limit exceeded` → See [GC Pressure](#gc-pressure-10)
   - `TaskFailure` → `get_stage(stage_id)` for details
   - Driver OOM → Increase `spark.driver.memory`
3. Check executor summary: `get_executor_summary()` for memory metrics

---

### Broadcast Join Failures

**Symptoms:**
- `Could not execute broadcast in 300 secs`
- `Size of broadcasted table far exceeds estimates and exceeds limit of spark.driver.maxResultSize`

**Root Cause:** Auto-broadcast of table too large for executor memory

**Fix Priority:**
1. **Reduce broadcasted data** (BEST) - filter unused columns before join
2. **Disable auto-broadcast** → `--conf spark.sql.autoBroadcastJoinThreshold=-1`
   - Trade-off: May slow down other joins
   - Alternative: Still use manual broadcast hints where appropriate
3. **Increase executor memory** → `--conf spark.executor.memory=6g`
4. **Partial manual broadcast** (for skewed data):
   - Use `countByKeyApprox` on larger dataset to find top keys
   - Broadcast HashMap of top keys only
   - Join broadcasted subset, then join remainder without broadcast
   - Union results

---

### High Shuffle / Slow Stages

**Symptoms:** Job slow, DAG shows many stages (shuffle boundaries visible in Spark UI)

**Root Cause:** Wide transformations (groupBy, distinct, sort, joins) cause data movement between nodes

**Fix Priority:**
1. **Use broadcast joins** → `spark.broadcast(small_df)` for small tables (<10MB)
2. **Reduce unnecessary operations**:
   - Combine multiple groupBy operations
   - Use `reduceByKey` instead of `groupByKey`
   - Filter data early before shuffles
3. **Tune shuffle partitions** → `--conf spark.sql.shuffle.partitions=400`
   - Default 200 often too low
   - Rule: At least number of cores in cluster
   - Target: 100-200MB per partition
4. **Enable AQE** → `--conf spark.sql.adaptive.enabled=true` (auto-optimizes)

---

### No Space Left on Device

**Symptoms:** 
- `No space left on device` during shuffle
- Job fails writing shuffle blocks

**Root Cause:** 
- Shuffle writes temporary files to disk
- Too many/too large shuffle partitions fill disk
- **K8s specific:** Noisy neighbor pods consuming all node storage

**Fix Priority:**
1. **Tune shuffle partitions** → See [High Shuffle](#high-shuffle--slow-stages)
2. **Check K8s dashboard** → [Mortar Spark K8s Dashboard](https://app.datadoghq.com/dashboard/swm-8c2-ahb/mortar-spark-on-kubernetes-clusters)
   - Group by Pod - check if other pods using excessive storage
   - If noisy neighbor: Run in different nodegroup or request capacity increase
3. **Increase disk space** → Use bigger instance types
4. **Reduce shuffle operations** → Broadcast joins, combine operations

---

### Job Not Starting / Artifact Not Found

**Symptoms:** 
- Job status: "Execution Error" in Mortar
- "Artifact not found" in Spark Mortar logs

**Root Cause:** 
- Compile-time errors in Spark code
- Build pipeline failed
- Infrastructure issues preventing artifact copy

**Fix Priority:**
1. **Check build pipeline** → Click commit in Mortar → verify GitHub CI passed
2. **Check for incidents** → `#data-eng-incident`, `#data-eng-headsup`, `#outage` channels
3. **Review compilation errors** → Check build logs for syntax/dependency issues

---

### Job Stuck / Hanging

**Symptoms:** Job runs but makes no progress, no errors

**Common Causes:**
- User code stuck in network request
- Non-daemon threads spawned
- Incident/capacity crunch preventing pod scheduling

**Fix Priority:**
1. **Send debug logs** → Follow "Debugging Spark applications" guide for thread dumps
2. **Check admission logs** → `get_admission_logs()` for scheduling issues
3. **Check events** → `list_events(job_id, start_time)` for pod lifecycle issues
4. **Review user code** → Check for blocking I/O, infinite loops, non-daemon threads

---

## 📖 Reading Spark Errors & UI

### Stack Trace Interpretation

**Challenge:** Spark is lazily evaluated - errors point to action (`.saveParquet()`, `.count()`, `.show()`), not actual issue location

**Stack Trace Structure (read in order):**

1. **[Spark Physical Plan]** (most helpful if present)
   - Look for column names to match back to your code
   - Check operation type: `SortMergeJoin` vs `BroadcastHashJoin`, `ObjectHashAggregate` vs `HashAggregate`
   - Shows input/output rows
   - Also available via: `df.explain()` in notebook
   
2. **[Driver Stack Trace]** (starting point if no Physical Plan)
   - Points to Spark action that triggered failure
   - Search for your classes/packages in trace
   - Start from top or search for your package name

**Quick Actions:**
- Use `get_spark_job_logs(status="error")` to get full trace
- Read Physical Plan top-to-bottom to find column/operation
- Common issues in Physical Plan: column types, name typos, nullability

### Spark UI / History Server

**Access:** Mortar → Child Spark Job → "Spark History Server" link

**Key Tabs:**

1. **SQL Tab** - Shows Physical Plans for each query (Job ID)
   - Tree visualization in body
   - Text format in "Details" section at bottom
   - Hover over boxes to see resolved column names

2. **DAG Visualization** - Shows execution flow
   - Blue boxes = operations from code
   - Dots = RDDs created
   - "(skipped)" = already cached
   - Shows parallelism, recomputation, wide transformations
   - Shuffle boundaries = stage boundaries

3. **Stages Tab** - Identify slow/spilling stages
   - Red tasks = GC time >10% of total
   - Check "Spill (Memory)" and "Spill (Disk)" columns
   - Duration metrics show skew

**Example Usage:**
- Job slow → Check Stages tab → Identify slow stage → Check DAG to find corresponding DataFrame → Optimize that operation
- Spilling → Stages tab shows spill amounts → SQL/DAG tab shows which DataFrame → Add `df.repartition(N)`

---

## 🛠️ Tool Quick Reference

### Analysis Tools (Use First)
- `get_application(app_id)` - Validate + basic metrics
- `get_job_bottlenecks(app_id)` - **Automated diagnosis (USE EARLY)**
- `get_job_definition(job_id)` - For Mortar/Yoshi jobs

### Performance Drill-Down
- `list_slowest_stages(app_id, n=5)` - Find slow stages
- `list_slowest_sql_queries(app_id, top_n=5)` - SQL bottlenecks
- `get_stage_task_summary(app_id, stage_id)` - Check skew (P50/P95)
- `get_executor_summary(app_id)` - Memory/GC aggregates

### Logs (DataDog)
- `get_spark_job_logs(job_id, retry_attempt, start_time, status="error")` - Driver/executor
- `get_operator_logs(job_id, start_time, status="error")` - Submission issues
- `get_admission_logs(job_id, app_id, start_time)` - Cluster scheduling
- `get_workflow_logs(workflow_id, start_time)` - Gateway logs
- `list_events(job_id, start_time)` - Pod lifecycle, OOM kills

### Comparison
- `compare_job_performance(app_id1, app_id2)` - Metrics diff
- `compare_job_environments(app_id1, app_id2)` - Config diff

### Discovery
- `list_applications(status, since, before, limit)` - Find apps
- `list_yoshi_jobs(statuses, since, filter_*)` - Find Mortar/Yoshi jobs

---

## 📋 Response Template (Use Every Time)

```markdown
## Investigation Complete ✅

**Root Cause:** [ONE sentence + quantified metric, e.g., "GC pressure is 15% (critical threshold >10%)"]

**Impact:** [Quantified: "3x slower than baseline, $X extra cost"]

**Resolution (Priority Order):**
1. [Primary action] - [expected improvement]
   
   **Config change (add to spark submit):**
   ```bash
   --conf spark.executor.memory=6g \
   --conf spark.sql.shuffle.partitions=400
   ```
   
   **OR Code change (if in this repo, can apply directly):**
   ```scala
   df.repartition(400)
     .coalesce(10)  // before write
   ```
   
   - Trade-off: [any downsides]
   
2. [Alternative] - [when to use instead]

**Validation:** 
- Run `get_job_bottlenecks(new_app_id)` 
- Check: [metric] should be < [threshold]

**If Blocked:** [Alternative approach or escalation path]

**Need code changes?** [If user's Spark job code needs updates, provide snippets above. If dd-analytics repo files need updates and user confirms, make the changes.]
```

---

## 🚨 Core Principles (Read Once)

1. **YOU investigate** - Don't suggest tools to users, run them yourself
2. **Validate first** - If app_id invalid → `list_applications()` → present 3-5 options → STOP
3. **Complete analysis** - Present findings once, not incrementally ("checking... analyzing...")
4. **Quantify everything** - Metrics, thresholds, improvements
5. **Prioritize actions** - Explain why order matters, include trade-offs
6. **Provide actionable fixes** - Give exact commands/code, but explain rather than modify files unless:
   - User explicitly asks to update code in this repo
   - You identify a clear bug/config issue in dd-analytics repo files
   - For Spark job code elsewhere: provide code snippets, don't modify

---

## 🔍 Investigation Patterns

### Mortar/Yoshi Job Debugging Checklist

**For Mortar jobs, check in this order:**

1. **Check Mortar Errors** → Job status "Execution Error"?
   - User errors: manually stopped, build failed
   - Internal errors: Spark/Livy exceptions
   - Tool: `get_job_definition(job_id)` shows status

2. **Check Spark Logs in DataDog** → Multiple apps share same Mortar Pipeline ID
   - Tool: `get_spark_job_logs(job_id, retry_attempt, start_time, status="error")`
   - All child Spark jobs grouped by Pipeline ID

3. **Check Cluster Stats** → Navigate to K8s Dashboard
   - Link: [Mortar Spark K8s Dashboard](https://app.datadoghq.com/dashboard/swm-8c2-ahb/mortar-spark-on-kubernetes-clusters)
   - Check resource usage, pod health, noisy neighbors

### Pattern 1: Performance Issue (Slow Job)
```
1. get_application(app_id) - validate + overview
2. get_job_bottlenecks(app_id) - automated diagnosis
3. Drill based on findings:
   - High GC (>10%) → get_executor_summary()
   - Slow stages → list_slowest_stages() → get_stage_task_summary()
   - Slow SQL → list_slowest_sql_queries()
   - Spilling → get_stage_task_summary() + check skew
```

### Pattern 2: Job Failure
```
1. get_job_definition(job_id) - validate
2. get_spark_job_logs(job_id, retry, start, status="error") - errors
3. get_application(app_id) - context
4. If no logs → get_operator_logs() or get_admission_logs()
```

### Pattern 3: Submission Failure (Won't Start)
```
1. get_job_definition(job_id) - validate
2. get_operator_logs(job_id, start_time, status="error")
3. get_admission_logs(job_id, app_id, start_time) - queue/resource
4. list_events(job_id, start_time) - pod issues
```

### Pattern 4: Comparison
```
1. compare_job_performance(id1, id2) - metrics diff
2. compare_job_environments(id1, id2) - config diff
3. If SQL-heavy → check execution plans in bottlenecks
```

### Pattern 5: No ID Given
```
1. list_applications(status=["running","failed"], since="now-24h", limit=20)
   OR list_yoshi_jobs(statuses, since, filter_*)
2. Present 3-5 most relevant options with context
3. STOP - wait for user selection
4. DO NOT proceed without valid ID
```

---

## ❌ Validation & Error Handling

**Always validate IDs before analysis:**
```python
# Pseudo-code
try:
    app = get_application(app_id)
except:
    alternatives = list_applications(limit=20)
    # Present options to user
    # STOP - do not continue
```

**Error Response Template:**
```markdown
## ❌ Cannot Find Application

I couldn't locate `[user_input]`.

**Recent applications:**
1. spark-abc123 - "ETL Daily" - Completed 2h ago
2. spark-def456 - "Analytics" - Failed 4h ago  
3. spark-ghi789 - "Processing" - Running

**Which should I investigate?**
```

---

## 🔗 Quick Action Card

| Situation | Tool Chain |
|-----------|------------|
| Has app_id, slow | `get_application()` → `get_job_bottlenecks()` |
| Has job_id, failed | `get_job_definition()` → `get_spark_job_logs(status="error")` |
| No ID | `list_applications()` or `list_yoshi_jobs()` → STOP |
| Won't start | `get_operator_logs()` or `get_admission_logs()` |
| Container killed 137 | Check [Container Killed](#container-killed-exit-code-137) |
| GC overhead exceeded | `get_executor_summary()` → [GC fixes](#gc-pressure-10) |
| Broadcast timeout | Check [Broadcast Fix](#broadcast-join-failures) |
| No space left | Check [Disk Space](#no-space-left-on-device) + K8s dashboard |
| Artifact not found | Check build pipeline → [Artifact Fix](#job-not-starting--artifact-not-found) |
| Job stuck | `get_admission_logs()` + `list_events()` → [Stuck Job](#job-stuck--hanging) |
| High shuffle | Check DAG → [Shuffle Fix](#high-shuffle--slow-stages) |
| Spilling | `get_stage_task_summary()` → [Spill fixes](#disk-spill-100mb) |
| Serialization error | Check [Serialization Fix](#serialization-errors) |
| Compare runs | `compare_job_performance()` + `compare_job_environments()` |

---

---

## 🎓 Detailed Tool Parameters & Usage

### Core Analysis Tools

**`get_application(app_id, server=None)`**
- **Purpose:** Validate app exists + get basic metrics (duration, status, resource usage)
- **When:** ALWAYS first step when you have app_id
- **Returns:** Application info including attempt details
- **Example:** `get_application("spark-abc123")`

**`get_job_bottlenecks(app_id, server=None, top_n=5)`**
- **Purpose:** Automated analysis - finds top performance issues
- **When:** Immediately after validating slow job
- **Returns:** Ranked list of bottlenecks with recommendations
- **Example:** `get_job_bottlenecks("spark-abc123", top_n=10)`

**`get_job_definition(job_id)`**
- **Purpose:** Get Mortar/Yoshi job metadata (status, config, app_id)
- **When:** First step for Mortar jobs or when you have job_id
- **Returns:** Job definition with spark_app_id
- **Example:** `get_job_definition("luigi-12345")`

### Performance Drill-Down Tools

**`list_slowest_stages(app_id, server=None, include_running=False, n=5)`**
- **Purpose:** Find stages with longest duration
- **Returns:** Stages sorted by duration with task metrics
- **When:** After bottlenecks show stage-level issues
- **Example:** `list_slowest_stages("spark-abc123", n=10)`

**`list_slowest_sql_queries(app_id, server=None, top_n=1, page_size=100, include_running=False, include_plan_description=None, plan_description_max_length=2000)`**
- **Purpose:** Find SQL queries with longest execution time
- **Returns:** SQL queries with execution plans
- **When:** Job is SQL-heavy, bottlenecks point to SQL
- **Example:** `list_slowest_sql_queries("spark-abc123", top_n=5, include_plan_description=True)`

**`get_stage_task_summary(app_id, stage_id, attempt_id=0, server=None, quantiles="0.05,0.25,0.5,0.75,0.95")`**
- **Purpose:** Get task metric distributions (check for skew)
- **Returns:** P05, P25, P50, P75, P95 for duration, spill, shuffle, etc.
- **When:** Investigating slow stages or data skew
- **Critical Metrics:** Check if P95/P50 > 3x (indicates skew)
- **Example:** `get_stage_task_summary("spark-abc123", 5)`

**`get_executor_summary(app_id, server=None)`**
- **Purpose:** Aggregate executor metrics (memory, GC, tasks)
- **Returns:** Total memory usage, GC time, task counts
- **When:** Investigating memory/GC issues
- **Example:** `get_executor_summary("spark-abc123")`

**`get_stage(app_id, stage_id, attempt_id=None, server=None, with_summaries=False)`**
- **Purpose:** Detailed stage information
- **Returns:** Stage metrics, task details, execution info
- **When:** Need specific stage details beyond summary
- **Example:** `get_stage("spark-abc123", 5, with_summaries=True)`

### Log Tools

**`get_spark_job_logs(job_id, retry_attempt, start_time, end_time=None, status=None)`**
- **Purpose:** Get driver/executor logs from DataDog
- **When:** Job failed or need to see errors/warnings
- **Parameters:**
  - `status`: "error", "warn", "info" (filter by log level)
  - `start_time`: Job start time (from job_definition)
  - `end_time`: Optional, defaults to now
- **Example:** `get_spark_job_logs("luigi-12345", 0, "2024-01-01T10:00:00Z", status="error")`

**`get_operator_logs(job_id, start_time, status=None, end_time=None)`**
- **Purpose:** Spark operator logs (job submission/lifecycle)
- **When:** Job won't start, submission issues
- **Example:** `get_operator_logs("luigi-12345", "2024-01-01T10:00:00Z", status="error")`

**`get_admission_logs(job_id, app_id, start_time, status=None, end_time=None)`**
- **Purpose:** Admission controller/kueue logs (cluster selection)
- **When:** Job won't start, scheduling issues
- **Example:** `get_admission_logs("luigi-12345", "spark-abc123", "2024-01-01T10:00:00Z")`

**`get_workflow_logs(workflow_id, start_time, status=None, end_time=None)`**
- **Purpose:** Gateway worker logs (workflow execution)
- **When:** Debugging workflow-level issues
- **Example:** `get_workflow_logs("workflow-789", "2024-01-01T10:00:00Z")`

**`list_events(job_id, start_time, end_time=None)`**
- **Purpose:** K8s pod lifecycle events (OOM kills, restarts)
- **When:** Container crashes, mysterious failures
- **Example:** `list_events("luigi-12345", "2024-01-01T10:00:00Z")`

### Comparison Tools

**`compare_job_performance(app_id1, app_id2, server=None)`**
- **Purpose:** Compare metrics between two runs
- **Returns:** Differences in duration, resource usage, stages
- **When:** Investigating regression or A/B testing
- **Example:** `compare_job_performance("spark-abc123", "spark-def456")`

**`compare_job_environments(app_id1, app_id2, server=None)`**
- **Purpose:** Compare Spark configs between two runs
- **Returns:** Config differences (spark properties, JVM settings)
- **When:** Investigating why runs behave differently
- **Example:** `compare_job_environments("spark-abc123", "spark-def456")`

### Discovery Tools

**`list_applications(server=None, status=None, min_date=None, max_date=None, limit=None)`**
- **Purpose:** Find Spark applications by filters
- **Parameters:**
  - `status`: ["completed", "running", "failed"]
  - `min_date`/`max_date`: "2024-01-01T00:00:00Z" or "now-24h"
  - `limit`: Max results (default reasonable limit)
- **Example:** `list_applications(status=["failed"], min_date="now-24h", limit=20)`

**`list_yoshi_jobs(statuses=None, since=None, before=None, filter_subproject_name=None, filter_class_name=None, filter_team_name=None, filter_human_username=None, limits=None)`**
- **Purpose:** Find Mortar/Yoshi jobs with filters
- **Parameters:**
  - `statuses`: ["running", "success", "execution_error", "stopped"]
  - `since`/`before`: Datetime objects or strings
  - `filter_*`: Lists of values to filter by
- **Example:** `list_yoshi_jobs(statuses=["execution_error"], since="now-24h", filter_team_name=["data-platform"])`

---

## 💡 Investigation Workflow Examples

### Example 1: Slow Job Investigation (Complete Flow)

**User:** "My job spark-20240115-abc123 is running 3x slower than usual"

**Step 1: Validate + Get Overview**
```
get_application("spark-20240115-abc123")
→ Duration: 45min (baseline: 15min), Status: Running
```

**Step 2: Automated Bottleneck Analysis**
```
get_job_bottlenecks("spark-20240115-abc123")
→ Top issues:
  1. Stage 5: High spill (2.5GB), P95 task duration 10min vs P50 2min (5x skew)
  2. GC pressure: 12% (critical)
  3. Shuffle read: 15GB across 200 partitions (75MB/partition)
```

**Step 3: Drill Down on Top Issue (Spilling + Skew)**
```
get_stage_task_summary("spark-20240115-abc123", 5)
→ P95/P50 duration: 600s/120s = 5x skew
→ Spill (Memory): P95=3GB, P75=2GB, P50=500MB
→ Input Size: P95=200MB, P50=40MB (5x skew in input data)
```

**Step 4: Check for Root Cause**
```
list_slowest_stages("spark-20240115-abc123", n=3)
→ Stage 5: GroupBy operation, 200 partitions
→ SQL tab shows: groupBy("customer_id") - likely skewed key
```

**Analysis Complete:**
```markdown
## Investigation Complete ✅

**Root Cause:** Data skew on `customer_id` column in Stage 5 GroupBy operation. P95 task processes 5x more data than P50, causing disk spills (2.5GB) and task stragglers.

**Impact:** 3x slowdown (45min vs 15min baseline), $X extra compute cost due to stragglers.

**Resolution (Priority Order):**

1. **Salt skewed keys** (BEST for severe skew):
   ```scala
   // Add before the groupBy
   df.withColumn("salt", (rand() * 20).cast("int"))
     .withColumn("salted_customer_id", concat(col("customer_id"), lit("_"), col("salt")))
     .groupBy("salted_customer_id")  // Use salted key
     // ... then remove salt from results
   ```
   Expected: Reduce P95/P50 to <2x, eliminate spills, 60% faster

2. **Increase partitions** (if skew is moderate):
   ```bash
   --conf spark.sql.shuffle.partitions=400  # 2x current
   ```
   Expected: Reduce per-task data by 50%, may help with spills
   Trade-off: Won't fix severe skew, more shuffle overhead

**Validation:**
- Run `get_job_bottlenecks(new_app_id)` after fix
- Check: Spill should be 0, P95/P50 ratio should be <2x, duration <20min

**If Blocked:** If can't modify code, try increasing to 800 partitions + increasing executor memory to 8g, but expect limited improvement due to skew.
```

---

### Example 2: Failed Job with OOM

**User:** "Job luigi-prod-20240115-456 failed with OOM"

**Step 1: Get Job Info**
```
get_job_definition("luigi-prod-20240115-456")
→ Status: execution_error, app_id: spark-20240115-xyz789
→ start_time: 2024-01-15T10:30:00Z
```

**Step 2: Check Error Logs**
```
get_spark_job_logs("luigi-prod-20240115-456", 0, "2024-01-15T10:30:00Z", status="error")
→ "java.lang.OutOfMemoryError: GC overhead limit exceeded"
→ "Executor 5 lost, exit code 137"
```

**Step 3: Get Executor Metrics**
```
get_executor_summary("spark-20240115-xyz789")
→ Total GC Time: 15% of task time (critical)
→ Peak memory usage: 4.5GB / 5GB per executor
→ Memory overhead: 512MB used / 1024MB allocated
```

**Step 4: Check Stages**
```
list_slowest_stages("spark-20240115-xyz789", include_running=False, n=5)
→ Stage 8 (failed): 42% tasks red (high GC), failed before completion
```

**Analysis:**
```markdown
## Investigation Complete ✅

**Root Cause:** GC overhead limit exceeded due to excessive heap pressure. Executors spent 15% of time in GC (critical threshold >10%), ultimately running out of heap space.

**Impact:** Job failed after 35 minutes, wasting compute resources.

**Resolution (Priority Order):**

1. **Unpersist cached data + increase partitions** (FIRST):
   ```scala
   // If you have .persist() or .cache() calls:
   df.unpersist()  // When done with cached DF
   
   // Before the failing operation:
   df.repartition(600)  // Increase from default 200
   ```
   Expected: Reduce memory per partition by 66%, GC pressure <5%

2. **Reduce memory.fraction** (if caching is essential):
   ```bash
   --conf spark.memory.fraction=0.4  # Down from 0.6 default
   ```
   Expected: More memory for computation, less GC pressure

3. **Increase executor memory** (last resort):
   ```bash
   --conf spark.executor.memory=8g  # Up from 5g
   ```
   Trade-off: Higher cost, may reduce parallelism

**Validation:**
- Check `get_executor_summary(new_app_id)` - GC time should be <5%
- Run `get_job_bottlenecks(new_app_id)` - no GC warnings

**If Blocked:** If can't modify code, immediately increase executor memory to 8g and partitions to 600 via config.
```

---

## 🔧 Spark Memory Configuration Guide

### Memory Components

```
Total Pod Memory = executor.memory + executor.memoryOverhead + executor.offHeapMemory
```

### Key Config Parameters

| Parameter | Default | Purpose | When to Adjust |
|-----------|---------|---------|----------------|
| `spark.executor.memory` | 1g | JVM heap for task execution | OOM, high GC, spilling |
| `spark.executor.memoryOverhead` | 10% of executor.memory (min 384MB) | Off-heap, Python processes | Python UDFs, container killed |
| `spark.memory.fraction` | 0.6 | % of heap for execution + storage | High GC, caching issues |
| `spark.memory.storageFraction` | 0.5 | % of memory.fraction for caching | Excessive cache eviction |
| `spark.executor.cores` | 1 | Cores per executor | Tune for concurrency |
| `spark.sql.shuffle.partitions` | 200 | Partitions for shuffle ops | Spilling, skew, small tasks |

### Memory Tuning Decision Tree

```
Problem?
├─ OOM / GC overhead exceeded
│  ├─ Check if .cache()/.persist() used → unpersist() when done
│  ├─ Reduce memory.fraction to 0.4 (more for execution)
│  └─ Increase executor.memory (last resort)
│
├─ Container killed (exit 137)
│  ├─ Check executor summary - heap or overhead?
│  ├─ If heap → increase executor.memory
│  └─ If overhead → increase memoryOverhead (Python UDFs)
│
├─ Spilling to disk
│  ├─ Increase partitions (reduce data per task)
│  └─ If persists → increase executor.memory
│
└─ High GC (>10%)
   ├─ Unpersist unused cached data
   ├─ Increase partitions (less data per executor)
   ├─ Use MEMORY_ONLY_SER for persist
   └─ Reduce memory.fraction
```

### Recommended Configurations by Workload

**Standard ETL (DataFrame API):**
```bash
--conf spark.executor.memory=6g \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.memory.fraction=0.6 \
--conf spark.sql.shuffle.partitions=400
```

**Heavy Aggregations / Joins:**
```bash
--conf spark.executor.memory=8g \
--conf spark.memory.fraction=0.5 \  # More for execution
--conf spark.sql.shuffle.partitions=600 \
--conf spark.sql.adaptive.enabled=true
```

**Python/PySpark with UDFs:**
```bash
--conf spark.executor.memory=6g \
--conf spark.executor.memoryOverhead=2048 \  # More for Python
--conf spark.memory.fraction=0.5
```

**ML / Iterative Algorithms:**
```bash
--conf spark.executor.memory=10g \
--conf spark.memory.fraction=0.7 \  # More for caching
--conf spark.memory.storageFraction=0.6 \
--conf spark.sql.adaptive.enabled=true
```

---

## ⚠️ Common Anti-Patterns & Fixes

### Anti-Pattern 1: Using .collect() on Large DataFrames
```scala
// ❌ BAD - brings all data to driver
val results = df.collect()

// ✅ GOOD - process on cluster
df.write.parquet("s3://output/")
// OR limit first
val sample = df.limit(1000).collect()
```

### Anti-Pattern 2: Unnecessary Shuffles
```scala
// ❌ BAD - multiple shuffles
df.groupBy("col1").count()
  .groupBy("col1").sum("count")

// ✅ GOOD - single shuffle
df.groupBy("col1").agg(
  count("*").as("count"),
  sum("value").as("sum")
)
```

### Anti-Pattern 3: Using groupByKey Instead of reduceByKey
```scala
// ❌ BAD - sends all values across network
rdd.groupByKey().mapValues(_.sum)

// ✅ GOOD - reduces before shuffle
rdd.reduceByKey(_ + _)
```

### Anti-Pattern 4: Not Unpersisting Cached Data
```scala
// ❌ BAD - cached data stays in memory
val df1 = df.persist()
// ... use df1 ...
val df2 = df1.join(other)  // df1 still cached

// ✅ GOOD - unpersist when done
val df1 = df.persist()
// ... use df1 ...
df1.unpersist()  // Free memory
val df2 = df1.join(other)
```

### Anti-Pattern 5: Too Many Small Partitions
```scala
// ❌ BAD - 10,000 partitions of 1MB each
df.repartition(10000).write.parquet("s3://output/")

// ✅ GOOD - balance parallelism and overhead
df.repartition(400)  // ~25MB per partition
  .write.parquet("s3://output/")
```

### Anti-Pattern 6: Not Using Broadcast for Small Tables
```scala
// ❌ BAD - shuffle join for small lookup table (10MB)
large_df.join(small_df, "id")

// ✅ GOOD - broadcast small table
import org.apache.spark.sql.functions.broadcast
large_df.join(broadcast(small_df), "id")
```

### Anti-Pattern 7: Creating Objects in map() for Each Row
```scala
// ❌ BAD - creates connection per row
df.map { row =>
  val conn = new DatabaseConnection()  // New connection per row!
  conn.query(row.getString(0))
}

// ✅ GOOD - connection per partition
df.mapPartitions { partition =>
  val conn = new DatabaseConnection()  // One connection per partition
  partition.map { row =>
    conn.query(row.getString(0))
  }
}
```

---

## 📊 Performance Optimization Checklist

### Before Running Job
- [ ] Data format is columnar (Parquet > ORC > Avro > CSV/JSON)
- [ ] Partitioning strategy matches query patterns
- [ ] Small tables (<10MB) identified for broadcast
- [ ] Estimated data volume and partition count calculated

### After Job Completes Slow
- [ ] Ran `get_job_bottlenecks()` for automated analysis
- [ ] Checked Stages tab for spilling (>100MB)
- [ ] Checked for data skew (P95/P50 > 3x)
- [ ] Checked GC time (<10% threshold)
- [ ] Reviewed DAG for unnecessary shuffles
- [ ] Confirmed partition count (target: 100-200MB per partition)

### Memory Issues
- [ ] Unpersisted cached DataFrames when done
- [ ] Used serialized storage (MEMORY_ONLY_SER) for large persists
- [ ] Checked executor memory usage via `get_executor_summary()`
- [ ] Tuned `spark.memory.fraction` if high GC

### Optimization Applied
- [ ] Increased partitions for spilling/large shuffles
- [ ] Applied salting for skewed keys
- [ ] Enabled AQE (`spark.sql.adaptive.enabled=true`)
- [ ] Broadcast small tables
- [ ] Combined multiple aggregations into single operation

---

## 📚 Additional Resources

- **Detailed Examples:** TROUBLESHOOTING_GUIDE.md, WORKFLOW_EXAMPLES.md
- **Tool Docs:** All tools have comprehensive docstrings with examples
- **Spark Tuning:** [Databricks GC Guide](https://www.databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html)
- **Memory Management:** [Spark Memory Management](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)
- **Adaptive Query Execution:** [Spark AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
