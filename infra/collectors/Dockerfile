# Pin to bookworm (Debian 12) variant — more explicit than bare :slim.
# Re-pin the digest in CI via: docker pull python:3.12-slim-bookworm && docker inspect --format='{{index .RepoDigests 0}}' python:3.12-slim-bookworm
FROM python:3.12-slim-bookworm

WORKDIR /app

# Install dependencies before copying source so this layer is cached independently
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy collector scripts and allowlist entrypoint
COPY gcp_collector.py aws_collector.py snowflake_collector.py mongodb_collector.py entrypoint.sh ./

# Non-root user — principle of least privilege (CIS Docker Benchmark 4.1)
RUN adduser --disabled-password --gecos "" appuser \
    && chmod +x /app/entrypoint.sh \
    && chown -R appuser:appuser /app

USER appuser

# Allowlist entrypoint: only gcp/aws/snowflake/mongodb_collector.py are permitted.
# Cloud Run Jobs pass the script name via --args, e.g. --args=gcp_collector.py
ENTRYPOINT ["/app/entrypoint.sh"]
