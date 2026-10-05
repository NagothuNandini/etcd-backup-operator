FROM python:3.11-slim

# etcdctl
ARG ETCD_VER=v3.5.15
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -L https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz \
    | tar xz -C /tmp \
 && mv /tmp/etcd-${ETCD_VER}-linux-amd64/etcdctl /usr/local/bin/ \
 && rm -rf /tmp/etcd-* /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etcd-backup-operator.py .

CMD ["kopf", "run", "--standalone", "-Av", "/app/etcd-backup-operator.py"]
