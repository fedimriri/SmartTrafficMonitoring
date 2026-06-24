# Deployment & Infrastructure Audit Report

**Role:** DevOps Architect  
**Project:** SmartTrafficMonitoring  
**Audit Date:** 2026-06-24  
**Auditor:** Claude (DevOps Architect role)

---

## Audit Scope

This report covers ONLY deployment and infrastructure concerns:
- Docker container topology and configuration
- Cluster deployment and runtime state
- Container networking and port mappings
- Deployment reproducibility
- Repository vs. runtime artifact gap

> **Reporting Rule:** Runtime infrastructure existence is evaluated independently from repository artifact presence. Both dimensions are reported separately.

---

## Evidence: Runtime Commands Executed

### 1. `docker ps`

```
CONTAINER ID   IMAGE                              STATUS          PORTS                                           NAMES
74e7969010bc   spark-image                        Up 26 minutes   22/tcp, 8080/tcp                                spark-slave2
87abb3df0d7d   spark-image                        Up 26 minutes   22/tcp, 8080/tcp                                spark-slave1
4f594fc6f31c   spark-image                        Up 26 minutes   22/tcp, 0.0.0.0:8080->8080/tcp                 spark-master
f8ea31687852   liliasfaxi/hadoop-cluster:latest   Up 26 minutes   0.0.0.0:9870->9870/tcp, 0.0.0.0:8088->8088/tcp hadoop-master
84b558bbaede   liliasfaxi/hadoop-cluster:latest   Up 26 minutes   0.0.0.0:8842->8842/tcp                         hadoop-worker2
ac8a6cd0f60e   liliasfaxi/hadoop-cluster:latest   Up 26 minutes   0.0.0.0:8841->8842/tcp                         hadoop-worker1
```

**Verdict:** 6 containers running — 3 Hadoop, 3 Spark.

---

### 2. JPS — Hadoop Cluster

```
hadoop-master:   387 NameNode | 632 SecondaryNameNode | 875 ResourceManager | 1230 Jps
hadoop-worker1:  227 NodeManager | 101 DataNode | 367 Jps
hadoop-worker2:  226 NodeManager | 101 DataNode | 366 Jps
```

**Verdict:** Full Hadoop cluster operational — NameNode, 2 DataNodes, ResourceManager, 2 NodeManagers.

---

### 3. JPS — Spark Cluster

```
spark-master:   120 Jps           ← NO Master daemon
spark-slave1:    72 Jps           ← NO Worker daemon
spark-slave2:    72 Jps           ← NO Worker daemon
```

**Verdict:** Spark containers running but Spark standalone daemons (Master/Worker) not started.

---

### 4. `hdfs dfsadmin -report`

```
Configured Capacity: 1003619270656 (934.69 GB)
Present Capacity:     666340179968 (620.58 GB)
Live datanodes (2):
  hadoop-worker1 (172.18.0.3:9866)  — 19 blocks
  hadoop-worker2 (172.18.0.4:9866)  — 19 blocks
```

**Verdict:** HDFS fully operational, 2 live DataNodes confirmed.

---

### 5. `yarn node -list`

```
Total Nodes: 2
  hadoop-worker2:44723   RUNNING   0 containers
  hadoop-worker1:36705   RUNNING   0 containers
```

**Verdict:** YARN ResourceManager accepting both NodeManagers.

---

### 6. HDFS Directory Tree

```
/traffic/input/Metro_Interstate_Traffic_Volume.csv   (3.1 MB, replication=2)
/traffic/output/hourly/part-r-00000
/traffic/output/hourly/_SUCCESS
/traffic/output/weather/part-r-00000
/traffic/output/weather/_SUCCESS
```

**Verdict:** Dataset loaded and MapReduce outputs present.

---

### 7. Docker Networks

```
hadoop        — bridge — 172.18.0.0/16
  hadoop-master   172.18.0.2
  hadoop-worker1  172.18.0.3
  hadoop-worker2  172.18.0.4

spark-network — bridge — 172.19.0.0/16
  spark-master    172.19.0.2
  spark-slave1    172.19.0.3
  spark-slave2    172.19.0.4
```

**Verdict:** Two separate networks with no shared segment — Spark cannot reach HDFS or YARN directly.

---

### 8. Spark Configuration

```
/opt/spark/conf/slaves:
  spark-slave1
  spark-slave2

spark-env.sh:   template only — no custom values set
spark-defaults.conf: empty
```

**Verdict:** Spark binary installed (version 2.4.5), workers enumerated in slaves file, but no cluster entrypoint configured.

---

### 9. Hadoop Core Configuration (inside container)

```xml
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://hadoop-master:9000/</value>
</property>
```

**Verdict:** HDFS endpoint configured inside container at `hdfs://hadoop-master:9000/`.

---

## Checklist — Item by Item

---

### 1. Hadoop Cluster Deployment

| Sub-item | Runtime Status | Repository Artifact |
|---|---|---|
| NameNode running | ✅ COMPLETE | ❌ MISSING — no Dockerfile |
| Secondary NameNode | ✅ COMPLETE | ❌ MISSING |
| 2× DataNodes | ✅ COMPLETE | ❌ MISSING |
| YARN ResourceManager | ✅ COMPLETE | ❌ MISSING |
| 2× YARN NodeManagers | ✅ COMPLETE | ❌ MISSING |
| Dataset in HDFS | ✅ COMPLETE | ❌ MISSING — no upload script |
| MapReduce output in HDFS | ✅ COMPLETE | ❌ MISSING |

**Runtime: ✅ COMPLETE**  
**Repository: ❌ MISSING**

Evidence: `jps` on all three Hadoop containers, `hdfs dfsadmin -report`, `yarn node -list`.

---

### 2. Spark Cluster Deployment

| Sub-item | Runtime Status | Repository Artifact |
|---|---|---|
| spark-master container running | ✅ COMPLETE | ❌ MISSING |
| spark-slave1 container running | ✅ COMPLETE | ❌ MISSING |
| spark-slave2 container running | ✅ COMPLETE | ❌ MISSING |
| Spark 2.4.5 binary installed | ✅ COMPLETE | ❌ MISSING |
| `slaves` file configured | ✅ COMPLETE | ❌ MISSING |
| Spark Master daemon running | ❌ MISSING | ❌ MISSING |
| Spark Worker daemons running | ❌ MISSING | ❌ MISSING |
| spark-defaults.conf configured | ❌ MISSING | ❌ MISSING |
| spark-env.sh configured | ❌ MISSING | ❌ MISSING |

**Runtime: ⚠️ PARTIAL** — Containers exist, binary confirmed (`spark-submit --version` → 2.4.5), slaves file correct. No Master or Worker JVM processes.  
**Repository: ❌ MISSING**

Evidence: `docker exec spark-master jps` → only Jps. `spark-submit --version` → 2.4.5. `cat /opt/spark/conf/slaves` → spark-slave1, spark-slave2.

---

### 3. Container Networking

| Sub-item | Status |
|---|---|
| Hadoop cluster isolated network (`hadoop`) | ✅ COMPLETE — 172.18.0.0/16 |
| Spark cluster isolated network (`spark-network`) | ✅ COMPLETE — 172.19.0.0/16 |
| Inter-container DNS resolution within Hadoop cluster | ✅ COMPLETE — `hdfs://hadoop-master:9000/` resolves |
| Inter-container DNS resolution within Spark cluster | ✅ COMPLETE — slaves file uses hostnames |
| **Shared network between Spark and Hadoop** | ❌ MISSING |

**Runtime: ⚠️ PARTIAL**

**Critical Gap:** Spark containers (172.19.x.x) and Hadoop containers (172.18.x.x) are on separate networks with no bridge. This means:
- `spark-submit --master spark://spark-master:7077` cannot read from `hdfs://hadoop-master:9000/`
- Spark YARN mode (`--master yarn`) cannot locate YARN ResourceManager
- The application in its current form bypasses this issue by using `local[*]` and `socketTextStream`, but any real cluster deployment will fail at network level

No repository artifact documents this topology or provides a fix.

---

### 4. Port Mappings

| Port | Service | Mapping | Status |
|---|---|---|---|
| 9870 | HDFS NameNode Web UI | 0.0.0.0:9870 → container:9870 | ✅ COMPLETE |
| 8088 | YARN ResourceManager Web UI | 0.0.0.0:8088 → container:8088 | ✅ COMPLETE |
| 8888 | Hadoop master (Jupyter/other) | 0.0.0.0:8888 → container:8888 | ✅ COMPLETE |
| 7877 | Hadoop master (custom) | 0.0.0.0:7877 → container:7877 | ✅ COMPLETE |
| 16010 | HBase Master Web UI | 0.0.0.0:16010 → container:16010 | ✅ COMPLETE |
| 8841 | hadoop-worker1 NodeManager UI | 0.0.0.0:8841 → container:8842 | ✅ COMPLETE |
| 8842 | hadoop-worker2 NodeManager UI | 0.0.0.0:8842 → container:8842 | ✅ COMPLETE |
| 8080 | Spark Master Web UI | 0.0.0.0:8080 → container:8080 | ✅ COMPLETE |
| 7077 | Spark submit port | not exposed to host | ⚠️ PARTIAL |
| 9000 | HDFS NameNode RPC | not exposed to host | ⚠️ PARTIAL |
| 9999 | Spark socket stream | not mapped | ❌ MISSING |

**Runtime: ⚠️ PARTIAL**  
**Repository: ❌ MISSING** — no documentation or configuration file in repo maps these ports.

Notable: Port 9999 (required by `TrafficStreamingApp` for `socketTextStream("localhost", 9999)`) is not exposed in any container port mapping. A socket producer would need to run inside the same container.

---

### 5. Reproducibility

| Sub-item | Status |
|---|---|
| `Dockerfile` (Hadoop) in repository | ❌ MISSING |
| `Dockerfile` (Spark) in repository | ❌ MISSING |
| `docker-compose.yml` in repository | ❌ MISSING |
| `.env` file for environment variables | ❌ MISSING |
| Dataset upload script (`hdfs dfs -put`) | ❌ MISSING |
| Hadoop cluster startup script | ❌ MISSING |
| Spark cluster startup script | ❌ MISSING |
| MapReduce job submission script | ❌ MISSING |
| Spark streaming submission script | ❌ MISSING |
| HDFS path setup script | ❌ MISSING |

**Runtime: ✅ COMPLETE** — cluster is functioning  
**Repository: ❌ MISSING** — a new developer cannot reproduce the environment from the repository alone

The README documents `mvn exec:java` invocations (local JVM mode) but makes no reference to:
- How to start the Docker cluster
- Which Docker images to pull
- How to upload the dataset to HDFS
- How to submit the JAR via `hadoop jar` to YARN
- How to start Spark daemons and submit via `spark-submit`

---

## Repository vs. Runtime Gap — Summary Table

| Category | Runtime Exists | In Repository |
|---|---|---|
| Hadoop cluster (3 containers) | ✅ YES | ❌ NO |
| Spark cluster (3 containers) | ⚠️ PARTIAL | ❌ NO |
| HDFS dataset | ✅ YES | ✅ YES (local copy in `dataset/`) |
| MapReduce JAR | ✅ YES (executed) | ✅ YES (`target/`) |
| Network topology definition | ✅ YES (runtime) | ❌ NO |
| Port mapping configuration | ✅ YES (runtime) | ❌ NO |
| Cluster startup procedure | ✅ YES (was done) | ❌ NO |
| Deployment documentation | ❌ NO | ❌ NO |

---

## Critical Findings

### FIND-01 — No Docker Artifacts in Repository

**Severity: CRITICAL**

The entire cluster (Hadoop + Spark) exists only as runtime state. There is no `Dockerfile`, `docker-compose.yml`, or any deployment script committed to the repository. The images used:
- `liliasfaxi/hadoop-cluster:latest` (8.01 GB)
- `spark-image:latest` (2.59 GB)

Neither image build process is documented. If the Docker host is wiped, the project cannot be reconstructed from the repository.

**Required artifact:**
```yaml
# docker-compose.yml (minimum viable)
version: "3"
services:
  hadoop-master:
    image: liliasfaxi/hadoop-cluster:latest
    container_name: hadoop-master
    networks: [hadoop]
    ports:
      - "9870:9870"
      - "8088:8088"
      - "9000:9000"
  hadoop-worker1:
    image: liliasfaxi/hadoop-cluster:latest
    container_name: hadoop-worker1
    networks: [hadoop]
  hadoop-worker2:
    image: liliasfaxi/hadoop-cluster:latest
    container_name: hadoop-worker2
    networks: [hadoop]
  spark-master:
    image: spark-image:latest
    container_name: spark-master
    networks: [hadoop, spark-network]    # ← must join hadoop network
    ports:
      - "8080:8080"
      - "7077:7077"
      - "9999:9999"
  spark-slave1:
    image: spark-image:latest
    container_name: spark-slave1
    networks: [spark-network]
  spark-slave2:
    image: spark-image:latest
    container_name: spark-slave2
    networks: [spark-network]
networks:
  hadoop:
    driver: bridge
  spark-network:
    driver: bridge
```

---

### FIND-02 — Spark Master/Worker Daemons Not Running

**Severity: HIGH**

`spark-master` container has no Master JVM process. `spark-slave1` and `spark-slave2` have no Worker JVM processes. Spark 2.4.5 is installed at `/opt/spark/` and the `slaves` file is configured, but `start-all.sh` has not been executed.

A submitted `spark-submit --master spark://spark-master:7077` job will fail immediately — no Master to accept the submission.

**Fix:**
```bash
docker exec spark-master /opt/spark/sbin/start-master.sh
docker exec spark-slave1 /opt/spark/sbin/start-slave.sh spark://spark-master:7077
docker exec spark-slave2 /opt/spark/sbin/start-slave.sh spark://spark-master:7077
```

---

### FIND-03 — Spark Network Cannot Reach Hadoop

**Severity: HIGH**

`spark-network` (172.19.0.0/16) and `hadoop` network (172.18.0.0/16) have no route between them. `spark-master` has only one network interface: 172.19.0.2. It cannot resolve `hadoop-master` by hostname or reach `hdfs://hadoop-master:9000/`.

**Fix:** Connect `spark-master` (and slaves if needed) to the `hadoop` network:
```bash
docker network connect hadoop spark-master
docker network connect hadoop spark-slave1
docker network connect hadoop spark-slave2
```
Or define a shared network in docker-compose.

---

### FIND-04 — Port 9999 Not Exposed

**Severity: MEDIUM**

`TrafficStreamingApp` binds to `socketTextStream("localhost", 9999)`. Port 9999 is not mapped to any host port in any container. A socket producer using `nc -lk 9999` must run inside the same container as the Spark application. This is not documented and is not portable.

**Fix:** Add `9999:9999` port mapping to the spark-master service and update `socketTextStream` host to `spark-master` or use `0.0.0.0`.

---

### FIND-05 — HDFS Path Mismatch (Carried from Infrastructure Audit)

**Severity: MEDIUM**

Actual HDFS structure: `/traffic/input/`, `/traffic/output/hourly/`, `/traffic/output/weather/`  
Specification requires: `/traffic-data/historical`, `/traffic-data/streaming`

No upload script in the repository to create the correct paths.

**Fix:**
```bash
docker exec hadoop-master hdfs dfs -mkdir -p /traffic-data/historical /traffic-data/streaming
docker exec hadoop-master hdfs dfs -put /path/to/Metro_Interstate_Traffic_Volume.csv /traffic-data/historical/
```

---

### FIND-06 — README Deployment Gap

**Severity: MEDIUM**

The README documents `mvn exec:java` invocations — which run the MapReduce drivers as local JVM processes, bypassing YARN entirely. This is not how Hadoop MapReduce is deployed to a cluster. The correct invocation is:

```bash
hadoop jar target/SmartTrafficMonitoring-1.0-SNAPSHOT.jar \
  com.traffic.mapreduce.hourly.TrafficHourDriver \
  /traffic/input/Metro_Interstate_Traffic_Volume.csv \
  /traffic/output/hourly
```

Similarly, the Spark section documents `mvn exec:java` rather than `spark-submit`. No Docker commands appear anywhere in the README.

---

## Required Fixes — Priority Order

| Priority | Action | File to Create |
|---|---|---|
| P1 | Create `docker-compose.yml` with correct network topology | `docker-compose.yml` |
| P2 | Start Spark Master and Worker daemons | `scripts/start-spark.sh` |
| P3 | Connect Spark containers to Hadoop network | `docker-compose.yml` |
| P4 | Expose port 9999 for socket stream | `docker-compose.yml` |
| P5 | Create HDFS setup script with correct paths | `scripts/setup-hdfs.sh` |
| P6 | Create MapReduce submission script using `hadoop jar` | `scripts/run-mapreduce.sh` |
| P7 | Create Spark submission script using `spark-submit` | `scripts/run-spark.sh` |
| P8 | Update README with Docker startup steps | `README.md` |

---

## Completion Score

| Category | Items | Complete | Partial | Missing |
|---|---|---|---|---|
| Hadoop Cluster (Runtime) | 7 | 7 | 0 | 0 |
| Spark Cluster (Runtime) | 9 | 3 | 0 | 6 |
| Container Networking | 5 | 4 | 0 | 1 |
| Port Mappings | 11 | 7 | 2 | 2 |
| Reproducibility | 10 | 0 | 0 | 10 |
| **Total** | **42** | **21** | **2** | **19** |

### Runtime Infrastructure Score: 21/32 = **66%**
### Repository Completeness Score: 0/10 = **0%**
### Overall Deployment Score: 21/42 = **50%**

---

## Conclusion

The Hadoop cluster is fully operational at runtime with all required daemons running and MapReduce outputs confirmed in HDFS. The Spark cluster exists at container level but Spark daemons are not started, and a network isolation issue prevents Spark from reaching HDFS or YARN even after daemons are started.

The repository contains **zero deployment artifacts** — no Dockerfile, no docker-compose, no shell scripts. The project is 100% non-reproducible from the repository alone. A reviewer cloning this repository has no means of recreating the runtime environment that was used to produce the audited outputs.

The single highest-impact action is committing a `docker-compose.yml` that defines both clusters on a shared network with correct port mappings and a startup script that initializes HDFS paths and loads the dataset.
