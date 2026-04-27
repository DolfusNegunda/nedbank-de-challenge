FROM nedbank-de-challenge/base:1.0

WORKDIR /app

# ── System packages missing from slim base ────────────────────────────────────
# procps  : provides 'ps', required by Spark's load-spark-env.sh
# hostname: required by Spark startup scripts
# curl    : already in base image but listed for clarity
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    hostname \
    && rm -rf /var/lib/apt/lists/*

# ── SPARK_HOME fix ────────────────────────────────────────────────────────────
# Base image sets SPARK_HOME=.../dist-packages/pyspark but pip installs to
# site-packages on python:3.11-slim-bookworm.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

# ── PYTHONPATH ────────────────────────────────────────────────────────────────
ENV PYTHONPATH=/app

# ── Spark network fix for --network=none ──────────────────────────────────────
# Prevents Spark JVM hostname DNS lookup failure under --network=none.
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost

# ── Pre-download Delta Lake JARs at build time ────────────────────────────────
# configure_spark_with_delta_pip() downloads JARs from Maven at runtime.
# Under --network=none this fails. We download the JARs during the build
# (when network IS available) and reference them as local file:// paths.
RUN mkdir -p /opt/delta-jars && \
    curl -fsSL -o /opt/delta-jars/delta-spark_2.12-3.1.0.jar \
        "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar" && \
    curl -fsSL -o /opt/delta-jars/delta-storage-3.1.0.jar \
        "https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar" && \
    echo "Delta JARs downloaded:" && ls -lh /opt/delta-jars/

# ── Additional Python dependencies ───────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Pipeline source code ──────────────────────────────────────────────────────
COPY pipeline/ pipeline/
COPY config/   config/

CMD ["python", "pipeline/run_all.py"]
