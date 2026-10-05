import os
import json
import time
import shutil
import threading
import subprocess
from datetime import datetime
import kopf
from croniter import croniter
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import boto3
from botocore.exceptions import BotoCoreError, ClientError

LOCAL_BACKUP_DIR = "/backup"
CLOUDIAN_ENDPOINT = os.getenv("CLOUDIAN_ENDPOINT")
CLOUDIAN_ACCESS_KEY = os.getenv("CLOUDIAN_ACCESS_KEY")
CLOUDIAN_SECRET_KEY = os.getenv("CLOUDIAN_SECRET_KEY")
CLOUDIAN_BUCKET = os.getenv("CLOUDIAN_BUCKET")
CLOUDIAN_PREFIX = os.getenv("CLOUDIAN_PREFIX", "etcd-backups").strip("/")
ETCDCTL_ENDPOINTS = os.getenv("ETCDCTL_ENDPOINTS", "https://127.0.0.1:2379")
ETCDCTL_CACERT = os.getenv("ETCDCTL_CACERT", "/etc/kubernetes/pki/etcd/ca.crt")
ETCDCTL_CERT = os.getenv("ETCDCTL_CERT", "/etc/kubernetes/pki/etcd/peer.crt")
ETCDCTL_KEY = os.getenv("ETCDCTL_KEY", "/etc/kubernetes/pki/etcd/peer.key")
MAX_RETRIES = 5
RETRY_DELAY = 10
DEFAULT_SCHEDULE = "0 */4 * * *"
POLL_INTERVAL = 60
FAILED_RETRY_DELAY = 600
RECOVERY_INTERVAL = 300
STATE_UPLOAD_PENDING = "UPLOAD_PENDING"
STATE_DELETE_PENDING = "DELETE_PENDING"
QUARANTINE_DIR = os.path.join(LOCAL_BACKUP_DIR, "quarantine")
backup_lock = threading.Lock()
last_recovery_check = 0.0
os.makedirs(LOCAL_BACKUP_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)


def timestamp():
    return datetime.utcnow().strftime("%Y%m%d-%H%M%S")


def state_file_for(snapshot_file):
    return snapshot_file + ".state"


def quarantine_name(original_name, reason):
    safe_reason = reason.replace(" ", "_").replace("/", "_").replace("\\", "_")
    return f"{original_name}.{safe_reason}.{timestamp()}"


def save_state(snapshot_file, state, bucket, object_key):
    state_file = state_file_for(snapshot_file)
    state_data = {
        "snapshot_file": snapshot_file,
        "state": state,
        "bucket": bucket,
        "object_key": object_key,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    temp_file = state_file + ".tmp"
    try:
        with open(temp_file, "w") as f:
            json.dump(state_data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, state_file)
        print(f"💾 State saved: {state_file} -> {state}")
        return True
    except Exception as e:
        print(f"❌ Failed to save state for {snapshot_file}: {e}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception:
            pass
        return False


def load_state(state_file):
    try:
        with open(state_file, "r") as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        print(f"❌ Corrupt state file {state_file}: {e}")
        return None
    except Exception as e:
        print(f"❌ Failed to read state file {state_file}: {e}")
        return None


def remove_state(snapshot_file):
    state_file = state_file_for(snapshot_file)
    try:
        if not os.path.exists(state_file):
            return True
        os.remove(state_file)
        print(f"🗑️ Removed state file: {state_file}")
        return True
    except Exception as e:
        print(f"⚠️ Failed to remove state file {state_file}: {e}")
        return False


def quarantine_state_file(state_file, reason, state_data=None):
    try:
        if not os.path.exists(state_file):
            print(f"⚠️ State file already missing: {state_file}")
            return True
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        original_name = os.path.basename(state_file)
        destination_name = quarantine_name(original_name, reason)
        destination = os.path.join(QUARANTINE_DIR, destination_name)
        quarantine_metadata = {
            "original_state_file": state_file,
            "reason": reason,
            "quarantined_at": datetime.utcnow().isoformat() + "Z",
            "state": state_data,
        }
        metadata_file = destination + ".metadata"
        with open(metadata_file, "w") as f:
            json.dump(quarantine_metadata, f, indent=2)
        shutil.move(state_file, destination)
        print(
            f"🚨 State file quarantined:\n"
            f"   Original : {state_file}\n"
            f"   Quarantine: {destination}\n"
            f"   Reason   : {reason}"
        )
        print("⚠️ Future backups are allowed to continue.")
        return True
    except Exception as e:
        print(f"❌ FAILED TO QUARANTINE {state_file}: {e}")
        return False


def create_etcd_snapshot(snapshot_file):
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"📸 Creating etcd snapshot (attempt {attempt}/{MAX_RETRIES})")
        if os.path.exists(snapshot_file):
            try:
                os.remove(snapshot_file)
            except Exception as e:
                print(f"⚠️ Could not remove old snapshot file: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue
        command = [
            "etcdctl",
            "--endpoints",
            ETCDCTL_ENDPOINTS,
            "--cacert",
            ETCDCTL_CACERT,
            "--cert",
            ETCDCTL_CERT,
            "--key",
            ETCDCTL_KEY,
            "snapshot",
            "save",
            snapshot_file,
        ]
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=600,
            )
            if result.returncode != 0:
                print(f"❌ etcdctl failed (attempt {attempt})")
                print(f"stderr: {result.stderr}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue
            if not os.path.exists(snapshot_file):
                print("❌ etcdctl returned success but snapshot file does not exist.")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue
            file_size = os.path.getsize(snapshot_file)
            if file_size <= 0:
                print("❌ Snapshot file exists but is empty.")
                try:
                    os.remove(snapshot_file)
                except Exception:
                    pass
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue
            print(f"✅ etcd snapshot created: {snapshot_file}")
            print(f"   Size: {file_size} bytes")
            return True
        except subprocess.TimeoutExpired:
            print(f"❌ etcdctl timed out (attempt {attempt})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"❌ Unexpected error while creating snapshot: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    print(f"❌ etcd snapshot creation failed after {MAX_RETRIES} attempts.")
    return False


def get_cloudian_client():
    return boto3.client(
        "s3",
        endpoint_url=CLOUDIAN_ENDPOINT,
        aws_access_key_id=CLOUDIAN_ACCESS_KEY,
        aws_secret_access_key=CLOUDIAN_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket_exists(s3_client):
    try:
        s3_client.head_bucket(Bucket=CLOUDIAN_BUCKET)
        return True
    except Exception:
        try:
            s3_client.create_bucket(Bucket=CLOUDIAN_BUCKET)
            print(f"✅ Created bucket: {CLOUDIAN_BUCKET}")
            return True
        except Exception as e:
            print(f"❌ Failed to create bucket {CLOUDIAN_BUCKET}: {e}")
            return False


def upload_to_cloudian(snapshot_file, object_key):
    if not os.path.exists(snapshot_file):
        print(f"❌ Cannot upload. Snapshot file does not exist: {snapshot_file}")
        return False
    try:
        s3_client = get_cloudian_client()
    except Exception as e:
        print(f"❌ Failed to create Cloudian client: {e}")
        return False
    if not ensure_bucket_exists(s3_client):
        return False
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"☁️ Uploading {snapshot_file} (attempt {attempt}/{MAX_RETRIES})")
        try:
            s3_client.upload_file(snapshot_file, CLOUDIAN_BUCKET, object_key)
            print(f"✅ Upload successful: {object_key}")
            return True
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"❌ Upload failed (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    print(f"❌ Upload failed after {MAX_RETRIES} attempts.")
    return False


def verify_cloudian_upload(snapshot_file, object_key):
    if not os.path.exists(snapshot_file):
        print(f"❌ Cannot verify because local snapshot is missing: {snapshot_file}")
        return False
    try:
        s3_client = get_cloudian_client()
        response = s3_client.head_object(Bucket=CLOUDIAN_BUCKET, Key=object_key)
        cloudian_size = response.get("ContentLength")
        local_size = os.path.getsize(snapshot_file)
        print("🔍 Verification:")
        print(f"   Local size    : {local_size}")
        print(f"   Cloudian size : {cloudian_size}")
        if cloudian_size != local_size:
            print("❌ Cloudian object size does not match local file.")
            return False
        print("✅ Cloudian upload verified.")
        return True
    except (ClientError, BotoCoreError, Exception) as e:
        print(f"❌ Cloudian verification failed: {e}")
        return False


def delete_local_snapshot(snapshot_file):
    for attempt in range(1, MAX_RETRIES + 1):
        print(
            f"🗑️ Deleting local snapshot "
            f"(attempt {attempt}/{MAX_RETRIES}): "
            f"{snapshot_file}"
        )
        if not os.path.exists(snapshot_file):
            print("✅ Snapshot already deleted.")
            return True
        try:
            os.remove(snapshot_file)
            if not os.path.exists(snapshot_file):
                print("✅ Local snapshot deleted.")
                return True
            print("⚠️ os.remove() returned but file still exists.")
        except Exception as e:
            print(f"❌ Delete failed (attempt {attempt}): {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
    print(f"❌ Failed to delete local snapshot after {MAX_RETRIES} attempts.")
    return False


def recover_upload_pending(state_file, state_data):
    snapshot_file = state_data.get("snapshot_file")
    bucket = state_data.get("bucket", CLOUDIAN_BUCKET)
    object_key = state_data.get("object_key")
    if not snapshot_file or not object_key:
        print(f"🚨 Invalid UPLOAD_PENDING state: {state_file}")
        quarantine_state_file(state_file, "invalid_state", state_data)
        return True
    if not os.path.exists(snapshot_file):
        print(f"🚨 UPLOAD_PENDING snapshot is missing:\n   {snapshot_file}")
        print("   This backup cannot be recovered.")
        print("   Moving state to quarantine so future backups are not blocked.")
        quarantine_state_file(state_file, "missing_snapshot", state_data)
        return True
    print(f"♻️ Recovering upload:\n   Snapshot: {snapshot_file}\n   Object  : {object_key}")
    uploaded = upload_to_cloudian(snapshot_file, object_key)
    if not uploaded:
        print("⚠️ Upload recovery failed.")
        print("   State remains UPLOAD_PENDING.")
        print("   It will be retried during the next scheduled recovery.")
        return False
    verified = verify_cloudian_upload(snapshot_file, object_key)
    if not verified:
        print("⚠️ Upload succeeded but verification failed.")
        print("   Keeping local snapshot.")
        print("   Keeping state as UPLOAD_PENDING.")
        return False
    if not save_state(snapshot_file, STATE_DELETE_PENDING, bucket, object_key):
        print("❌ Failed to update state to DELETE_PENDING.")
        return False
    deleted = delete_local_snapshot(snapshot_file)
    if not deleted:
        print("⚠️ Snapshot could not be deleted.")
        print("   State remains DELETE_PENDING.")
        return False
    remove_state(snapshot_file)
    print("✅ Upload recovery completed.")
    return True


def recover_delete_pending(state_file, state_data):
    snapshot_file = state_data.get("snapshot_file")
    object_key = state_data.get("object_key")
    if not snapshot_file or not object_key:
        print(f"🚨 Invalid DELETE_PENDING state: {state_file}")
        quarantine_state_file(state_file, "invalid_state", state_data)
        return True
    if not os.path.exists(snapshot_file):
        print(f"ℹ️ Snapshot already deleted: {snapshot_file}")
        print("   Removing completed state.")
        remove_state(snapshot_file)
        return True
    print(f"♻️ Recovering delete:\n   {snapshot_file}")
    deleted = delete_local_snapshot(snapshot_file)
    if not deleted:
        print("⚠️ Delete recovery failed.")
        print("   State remains DELETE_PENDING.")
        return False
    remove_state(snapshot_file)
    print("✅ Delete recovery completed.")
    return True


def recover_pending_backups():
    print("🔎 Checking for pending backups...")
    try:
        state_files = [
            os.path.join(LOCAL_BACKUP_DIR, filename)
            for filename in os.listdir(LOCAL_BACKUP_DIR)
            if filename.endswith(".db.state")
        ]
    except Exception as e:
        print(f"❌ Failed to scan backup directory: {e}")
        return True
    if not state_files:
        print("✅ No pending backups found.")
        return True
    print(f"📋 Found {len(state_files)} state file(s).")
    all_recovered = True
    for state_file in sorted(state_files):
        print("\n" + "=" * 70)
        print(f"Processing state: {state_file}")
        print("=" * 70)
        state_data = load_state(state_file)
        if state_data is None:
            print("🚨 State file is unreadable.")
            quarantined = quarantine_state_file(state_file, "corrupt_state", None)
            if not quarantined:
                all_recovered = False
            continue
        state = state_data.get("state")
        if state == STATE_UPLOAD_PENDING:
            recovered = recover_upload_pending(state_file, state_data)
            if not recovered:
                all_recovered = False
        elif state == STATE_DELETE_PENDING:
            recovered = recover_delete_pending(state_file, state_data)
            if not recovered:
                all_recovered = False
        else:
            print(f"🚨 Unknown backup state: {state}")
            quarantined = quarantine_state_file(state_file, "unknown_state", state_data)
            if not quarantined:
                all_recovered = False
    print("\n" + "=" * 70)
    if all_recovered:
        print("✅ Pending backup recovery completed.")
    else:
        print("⚠️ Some pending backups could not be recovered.")
        print("   They remain persisted for the next recovery cycle.")
    print("=" * 70)
    return True


def take_snapshot():
    print("\n" + "=" * 70)
    print("STEP 1: Recover pending backups")
    print("=" * 70)
    recover_pending_backups()
    print("\n" + "=" * 70)
    print("STEP 2: Create new etcd snapshot")
    print("=" * 70)
    snapshot_name = f"etcd-{timestamp()}.db"
    snapshot_file = os.path.join(LOCAL_BACKUP_DIR, snapshot_name)
    object_key = (
        f"{CLOUDIAN_PREFIX}/{snapshot_name}" if CLOUDIAN_PREFIX else snapshot_name
    )
    created = create_etcd_snapshot(snapshot_file)
    if not created:
        print("❌ Snapshot creation failed after all retries.")
        print("   Nothing will be uploaded.")
        print("   Aborting this backup run.")
        return False
    print("\n" + "=" * 70)
    print("STEP 3: Save UPLOAD_PENDING state")
    print("=" * 70)
    state_saved = save_state(
        snapshot_file, STATE_UPLOAD_PENDING, CLOUDIAN_BUCKET, object_key
    )
    if not state_saved:
        print("🚨 CRITICAL:")
        print("   Snapshot exists but its state could not be persisted.")
        print(
            "   NOT uploading because we cannot "
            "reliably recover the file if upload fails."
        )
        return False
    print("\n" + "=" * 70)
    print("STEP 4: Upload snapshot")
    print("=" * 70)
    uploaded = upload_to_cloudian(snapshot_file, object_key)
    if not uploaded:
        print(f"⚠️ Upload failed after {MAX_RETRIES} attempts.")
        print("   Local snapshot is PRESERVED.")
        print("   State remains UPLOAD_PENDING.")
        print("   Future recovery will retry the SAME snapshot.")
        return False
    print("\n" + "=" * 70)
    print("STEP 5: Verify Cloudian object")
    print("=" * 70)
    verified = verify_cloudian_upload(snapshot_file, object_key)
    if not verified:
        print("⚠️ Upload verification failed.")
        print("   Local snapshot is PRESERVED.")
        print("   State remains UPLOAD_PENDING.")
        return False
    print("\n" + "=" * 70)
    print("STEP 6: Change state to DELETE_PENDING")
    print("=" * 70)
    state_saved = save_state(
        snapshot_file, STATE_DELETE_PENDING, CLOUDIAN_BUCKET, object_key
    )
    if not state_saved:
        print("🚨 CRITICAL:")
        print("   Cloudian upload is verified.")
        print("   But DELETE_PENDING state could not be saved.")
        print("   Local snapshot will be preserved.")
        return False
    print("\n" + "=" * 70)
    print("STEP 7: Delete local snapshot")
    print("=" * 70)
    deleted = delete_local_snapshot(snapshot_file)
    if not deleted:
        print("⚠️ Local deletion failed.")
        print("   Cloudian copy is already verified.")
        print("   State remains DELETE_PENDING.")
        print("   Future recovery will retry deletion.")
        return False
    print("\n" + "=" * 70)
    print("STEP 8: Remove completed state")
    print("=" * 70)
    state_removed = remove_state(snapshot_file)
    if not state_removed:
        print("⚠️ Backup itself is complete.")
        print("   However, state file could not be removed.")
        print(
            "   Future recovery will notice that "
            "the snapshot is already gone."
        )
        return True
    print("\n" + "=" * 70)
    print("🎉 BACKUP COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print(f"Snapshot : {snapshot_file}")
    print(f"Cloudian : s3://{CLOUDIAN_BUCKET}/{object_key}")
    return True


def utc_now_iso():
    return datetime.utcnow().isoformat() + "Z"


def parse_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).rstrip("Z"))
    except ValueError:
        return None


def backup_is_due(schedule, status):
    now = datetime.utcnow()
    previous_due = croniter(schedule, now).get_prev(datetime)
    last_backup = parse_iso(status.get("lastBackup"))
    if last_backup and last_backup >= previous_due:
        return False
    last_attempt = parse_iso(status.get("lastBackupAttempt"))
    if last_attempt and (now - last_attempt).total_seconds() < FAILED_RETRY_DELAY:
        return False
    return True


def recover_if_due():
    global last_recovery_check
    now = time.monotonic()
    if now - last_recovery_check < RECOVERY_INTERVAL:
        return
    last_recovery_check = now
    recover_pending_backups()


@kopf.on.create("clusterops.io", "v1alpha1", "etcdbackups")
def create_backup(spec, name, patch, **kwargs):
    print(f"🚀 EtcdBackup resource created: {name}")
    acquired = backup_lock.acquire(blocking=False)
    if not acquired:
        print("⚠️ Another backup is already running.")
        raise kopf.TemporaryError(
            "Another backup is already in progress.", delay=RETRY_DELAY
        )
    try:
        success = take_snapshot()
        if success:
            patch.status["phase"] = "Completed"
            patch.status["lastBackup"] = utc_now_iso()
            print(f"✅ EtcdBackup {name} completed.")
        else:
            patch.status["phase"] = "Failed"
            patch.status["lastBackupAttempt"] = utc_now_iso()
            print(f"❌ Backup process failed for {name}.")
    finally:
        backup_lock.release()


@kopf.on.timer("clusterops.io", "v1alpha1", "etcdbackups", interval=POLL_INTERVAL)
def scheduled_backup(spec, status, name, patch, **kwargs):
    schedule = spec.get("schedule") or DEFAULT_SCHEDULE
    if not croniter.is_valid(schedule):
        if status.get("phase") != "InvalidSchedule":
            print(f"❌ Invalid cron schedule for {name}: {schedule!r}")
        patch.status["phase"] = "InvalidSchedule"
        return
    acquired = backup_lock.acquire(blocking=False)
    if not acquired:
        return
    try:
        recover_if_due()
        if not backup_is_due(schedule, status):
            return
        print(f"⏰ Scheduled backup due for {name} (schedule: {schedule})")
        success = take_snapshot()
        if success:
            patch.status["phase"] = "Completed"
            patch.status["lastBackup"] = utc_now_iso()
            print(f"✅ Scheduled backup completed: {name}")
        else:
            patch.status["phase"] = "Failed"
            patch.status["lastBackupAttempt"] = utc_now_iso()
            print(f"⚠️ Scheduled backup failed: {name}")
    finally:
        backup_lock.release()


@kopf.on.startup()
def startup(**kwargs):
    print("\n" + "=" * 70)
    print("🚀 Etcd Backup Operator starting...")
    print("=" * 70)
    os.makedirs(LOCAL_BACKUP_DIR, exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    acquired = backup_lock.acquire(blocking=False)
    if not acquired:
        print("⚠️ Could not acquire startup backup lock.")
        return
    try:
        recover_pending_backups()
    finally:
        backup_lock.release()
    print("✅ Etcd Backup Operator started.")


@kopf.on.delete("clusterops.io", "v1alpha1", "etcdbackups")
def delete_backup_resource(name, **kwargs):
    print(f"🗑️ EtcdBackup resource deleted: {name}")

