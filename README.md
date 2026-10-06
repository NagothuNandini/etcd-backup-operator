# etcd-backup-operator

A Kubernetes operator (Python + [Kopf](https://kopf.readthedocs.io)) that takes scheduled **etcd snapshots**, uploads them to **Cloudian (S3-compatible) storage**, verifies the upload, and cleans up local files. It is crash-safe: interrupted backups are recovered automatically on the next run.

```
EtcdBackup CR -> Operator -> etcdctl snapshot save -> /backup (PVC)
              -> upload to Cloudian -> verify size -> delete local copy
```

---

## Table of contents

1. [Repository layout](#1-repository-layout)
2. [Prerequisites](#2-prerequisites)
3. [Configure before applying](#3-configure-before-applying)
4. [Deploy (apply in order)](#4-deploy-apply-in-order)
5. [Verify the installation](#5-verify-the-installation)
6. [Trigger and schedule backups](#6-trigger-and-schedule-backups)
7. [Verify a backup reached Cloudian](#7-verify-a-backup-reached-cloudian)
8. [How it works](#8-how-it-works)
9. [Troubleshooting](#9-troubleshooting)
10. [Uninstall](#10-uninstall)
11. [Optional: build your own image](#11-optional-build-your-own-image)

---

## 1. Repository layout

| File | Purpose |
|---|---|
| `rbac.yaml` | Namespace `etcd-backup`, ServiceAccount, ClusterRole, ClusterRoleBinding |
| `crd.yaml` | Defines the `EtcdBackup` custom resource (`clusterops.io/v1alpha1`) |
| `pvc.yaml` | 1Gi persistent volume mounted at `/backup` |
| `secret.yaml` | Cloudian access key and secret key |
| `deploy.yaml` | Operator Deployment (runs on a control-plane node) |
| `cr.yaml` | An `EtcdBackup` instance (sets the schedule) |
| `Dockerfile` | Builds the operator image (Python 3.11 + etcdctl v3.5.15) |
| `requirements.txt` | Python dependencies |
| `etcd-backup-operator.py` | Operator source code |

---

## 2. Prerequisites

- A **kubeadm-style cluster** where etcd runs on the control-plane node and its certificates are in `/etc/kubernetes/pki/etcd`.
- `kubectl` configured with **cluster-admin** rights (needed to create the CRD and ClusterRole).
- A **Cloudian / S3-compatible** endpoint, a bucket name, and an access key and secret key with permission to `HeadBucket`, `CreateBucket` (optional), `PutObject`, and `HeadObject`.
- A StorageClass that can provision a `ReadWriteOnce` volume (or a default StorageClass).
- Network access from the control-plane node to the Cloudian endpoint.
- Access to pull the image `nandini951/etcd-backup-operator:2.0` (or your own build, see [section 11](#11-optional-build-your-own-image)).

Quick checks:

```bash
kubectl get nodes -l node-role.kubernetes.io/control-plane
kubectl get storageclass
ls /etc/kubernetes/pki/etcd        # run on the control-plane node
```

---

## 3. Configure before applying

### 3.1 Cloudian endpoint and bucket: `deploy.yaml`

Edit these values in the `env:` section:

```yaml
- name: CLOUDIAN_ENDPOINT
  value: "https://mycloudendpoint.com"   # <-- your Cloudian S3 endpoint
- name: CLOUDIAN_BUCKET
  value: "etcdbackupbucket"              # <-- your bucket name
```

Optional environment variables (add under `env:` if you want to change the defaults):

| Variable | Default | Meaning |
|---|---|---|
| `CLOUDIAN_PREFIX` | `etcd-backups` | Folder/prefix inside the bucket |
| `ETCDCTL_ENDPOINTS` | `https://127.0.0.1:2379` | etcd endpoint |
| `ETCDCTL_CACERT` | `/etc/kubernetes/pki/etcd/ca.crt` | etcd CA cert |
| `ETCDCTL_CERT` | `/etc/kubernetes/pki/etcd/peer.crt` | client cert |
| `ETCDCTL_KEY` | `/etc/kubernetes/pki/etcd/peer.key` | client key |

### 3.2 Credentials: `secret.yaml`

The Secret must be named **`cloudian-credentials`**, live in namespace **`etcd-backup`**, and contain the keys `CLOUDIAN_ACCESS_KEY` and `CLOUDIAN_SECRET_KEY`.

Either edit `secret.yaml`, or (recommended, so keys never land in Git) create it from the command line **after** step 1 below creates the namespace:

```bash
kubectl create secret generic cloudian-credentials \
  --namespace etcd-backup \
  --from-literal=CLOUDIAN_ACCESS_KEY='<your-access-key>' \
  --from-literal=CLOUDIAN_SECRET_KEY='<your-secret-key>'
```

> If you use this command, **skip** `kubectl apply -f secret.yaml` in the deploy steps. Never commit real keys to Git.

### 3.3 Storage class: `pvc.yaml`

If your cluster has no default StorageClass, uncomment and set:

```yaml
storageClassName: <your-storage-class>
```

Adjust `storage: 1Gi` if needed. Snapshots are deleted after upload, but one snapshot plus any failed or pending snapshots must fit.

### 3.4 Schedule: `cr.yaml`

Set `spec.schedule` to a 5-field cron expression, evaluated in **UTC**. Default: `0 */4 * * *` (every 4 hours).

---

## 4. Deploy (apply in order)

Order matters: the namespace must exist before namespaced objects, and the CRD must exist before the custom resource.

### Step 1: Namespace, ServiceAccount, RBAC

```bash
kubectl apply -f rbac.yaml
```

Creates namespace `etcd-backup`, ServiceAccount `etcd-backup-operator`, and the ClusterRole and binding.

### Step 2: Custom Resource Definition

```bash
kubectl apply -f crd.yaml
kubectl get crd etcdbackups.clusterops.io
```

### Step 3: Persistent volume claim

```bash
kubectl apply -f pvc.yaml
kubectl get pvc -n etcd-backup
```

> The PVC may show `Pending` until the pod is scheduled if your StorageClass uses `WaitForFirstConsumer`. That is normal.

### Step 4: Cloudian credentials

```bash
kubectl apply -f secret.yaml
# OR use the kubectl create secret command from section 3.2
kubectl get secret cloudian-credentials -n etcd-backup
```

### Step 5: Operator Deployment

```bash
kubectl apply -f deploy.yaml
kubectl rollout status deployment/etcd-backup-operator -n etcd-backup
```

### Step 6: Create the EtcdBackup resource

```bash
kubectl apply -f cr.yaml
```

Creating this resource triggers an **immediate first backup**; after that, the cron schedule takes over.

### One-shot version

```bash
kubectl apply -f rbac.yaml
kubectl apply -f crd.yaml
kubectl apply -f pvc.yaml
kubectl apply -f secret.yaml
kubectl apply -f deploy.yaml
kubectl apply -f cr.yaml
```

---

## 5. Verify the installation

```bash
# Operator pod should be Running, on a control-plane node
kubectl get pods -n etcd-backup -o wide

# Follow the logs
kubectl logs -n etcd-backup deploy/etcd-backup-operator -f

# Check the custom resource (shortname: eb)
kubectl get etcdbackup -A
kubectl get eb -A
```

Expected `kubectl get eb` output:

```
NAME        SCHEDULE      PHASE       LASTBACKUP             AGE
<name>      0 */4 * * *   Completed   2026-10-06T10:00:00Z   5m
```

Phases you may see:

| Phase | Meaning |
|---|---|
| `Completed` | Last backup succeeded |
| `Failed` | Last attempt failed (retried automatically after 10 minutes) |
| `InvalidSchedule` | The cron expression in `spec.schedule` is invalid |

A successful run ends in the logs with:

```
BACKUP COMPLETED SUCCESSFULLY
Cloudian : s3://<bucket>/<prefix>/etcd-YYYYMMDD-HHMMSS.db
```

---

## 6. Trigger and schedule backups

**Automatic:** the operator checks every 60 seconds whether a backup is due based on `spec.schedule`.

**Manual / on-demand:** creating a new `EtcdBackup` resource runs a backup immediately.

```bash
kubectl delete -f cr.yaml
kubectl apply -f cr.yaml
```

**Change the schedule:**

```bash
kubectl edit etcdbackup <name>
# change spec.schedule, e.g. "0 2 * * *" for daily at 02:00 UTC
```

Common cron examples (UTC):

| Cron | Meaning |
|---|---|
| `0 */4 * * *` | Every 4 hours (default) |
| `0 * * * *` | Hourly |
| `0 2 * * *` | Daily at 02:00 |
| `0 */12 * * *` | Every 12 hours |

---

## 7. Verify a backup reached Cloudian

Use any S3 client against your Cloudian endpoint, for example the AWS CLI:

```bash
aws s3 ls s3://<bucket>/etcd-backups/ --endpoint-url https://<your-cloudian-endpoint>
```

You should see objects like `etcd-20261006-100000.db`.

Check the local backup directory is clean (completed backups are removed locally):

```bash
kubectl exec -n etcd-backup deploy/etcd-backup-operator -- ls -la /backup
```

Normally it is empty, except for the `quarantine/` folder. Leftover `.db` and `.db.state` files mean a backup is pending and will be retried.

---

## 8. How it works

**Backup pipeline (8 steps):**

1. Recover any pending backups from earlier runs
2. Take the snapshot with `etcdctl snapshot save` (5 attempts, 10 s apart)
3. Save `UPLOAD_PENDING` state file
4. Upload to Cloudian (5 attempts)
5. Verify: Cloudian object size must equal the local file size
6. Switch state to `DELETE_PENDING`
7. Delete the local snapshot
8. Remove the state file

**State machine:**

```
UPLOAD_PENDING --(upload + verify OK)--> DELETE_PENDING --(local file deleted)--> state removed
```

**Recovery** runs at pod startup, before every backup, and every 5 minutes. State files that are corrupt, unknown, or whose snapshot is missing are moved to `/backup/quarantine/` so they never block future backups.

**Design constraints:**

- `replicas: 1` is required, because the backup lock is in-process only.
- `strategy: Recreate` is used because the PVC is ReadWriteOnce.
- `hostNetwork: true`, a control-plane `nodeSelector`, a `hostPath` mount of the etcd certs, and `runAsUser: 0` are needed so the pod can reach etcd and read its root-only keys.

---

## 9. Troubleshooting

| Symptom | Check / fix |
|---|---|
| Pod stuck `Pending` | `kubectl describe pod -n etcd-backup <pod>`. Check the PVC is bound and a control-plane node is available. |
| Pod stuck `ImagePullBackOff` | Check the image name and registry access, or build and push your own image (section 11). |
| Pod restarting repeatedly | See the liveness probe note below. |
| `etcdctl failed` in logs | Verify cert paths and that `/etc/kubernetes/pki/etcd` exists on the node. Confirm etcd listens on `127.0.0.1:2379`. |
| Upload keeps failing | Check endpoint URL, credentials, bucket permissions, and network reachability from the control-plane node. |
| Phase `Failed` | Read the operator logs. It retries after 10 minutes (and pending uploads are retried every 5 minutes). |
| Phase `InvalidSchedule` | Fix `spec.schedule` (5-field cron). |
| `.db` and `.state` files remain in `/backup` | A backup is pending. Check logs, as recovery retries automatically. |
| Files in `/backup/quarantine` | A state file was bad. Read the matching `.metadata` file for the reason. |

**Liveness probe note:** `deploy.yaml` defines a liveness probe on `http://:8085/healthz`. Kopf only serves that endpoint if it is started with a liveness flag. The Dockerfile's default command is:

```
kopf run --standalone -Av /app/etcd-backup-operator.py
```

If the pod is restarted repeatedly by the probe, either remove the `livenessProbe` block from `deploy.yaml`, or add the flag to the container command, for example:

```yaml
command: ["kopf", "run", "--standalone", "-Av",
          "--liveness=http://0.0.0.0:8085/healthz",
          "/app/etcd-backup-operator.py"]
```

Useful commands:

```bash
kubectl describe pod -n etcd-backup -l app=etcd-backup-operator
kubectl logs -n etcd-backup deploy/etcd-backup-operator --previous
kubectl get events -n etcd-backup --sort-by=.lastTimestamp
kubectl describe etcdbackup <name>
```

---

## 10. Uninstall

```bash
kubectl delete -f cr.yaml
kubectl delete -f deploy.yaml
kubectl delete -f secret.yaml      # or: kubectl delete secret cloudian-credentials -n etcd-backup
kubectl delete -f pvc.yaml
kubectl delete -f crd.yaml
kubectl delete -f rbac.yaml        # also deletes the etcd-backup namespace
```

> Deleting the CRD removes all `EtcdBackup` resources. Backups already in Cloudian are **not** deleted.

---

## 11. Optional: build your own image

```bash
docker build -t <your-registry>/etcd-backup-operator:2.0 .
docker push <your-registry>/etcd-backup-operator:2.0
```

Then update `image:` in `deploy.yaml`:

```yaml
image: <your-registry>/etcd-backup-operator:2.0
```

---

## Notes

- **Retention:** the operator does not delete old backups from Cloudian. Configure a lifecycle or expiry rule on the bucket.
- **Restore:** this operator only creates backups. Restoring uses `etcdctl snapshot restore` and should be rehearsed in a test cluster before you need it.
- **Security:** keep Cloudian keys out of Git, and restrict who can read the `cloudian-credentials` Secret.
