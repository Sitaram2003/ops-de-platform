# Airflow on k3s (AWS EC2) — Infrastructure Fixes & Runbook

**Environment:** Single-node k3s on EC2 (`m7i.2xlarge`), AWS account `940507692220`, region `ap-south-2`
**Stack:** Airflow 3.2.2 (KubernetesExecutor), External Secrets Operator (ESO), git-sync, Bitnami PostgreSQL

---

## 1. Git-sync SSH key via AWS Secrets Manager + ESO ClusterSecretStore (base64)

### Problem
`git-sync-init` repeatedly failed with:
```
Load key "/etc/git-secret/ssh": error in libcrypto
git@github.com: Permission denied (publickey)
```
despite the SSH key being valid and correctly mounted.

### Root cause
Storing a **multi-line PEM private key directly** in AWS Secrets Manager is fragile. The newline-sensitive PEM format gets corrupted by nearly any text-rendering boundary it passes through — terminal display, AWS Console textarea paste, cross-machine copy (Windows ↔ EC2), or even the AWS CLI's own `--output text` formatter. We repeatedly "fixed" the key only to have corruption reintroduced by the very next hop.

### Fix — encode as base64 before storing, decode via ESO on sync

**Store the key (base64-encoded, single line, nothing left to corrupt):**
```bash
tr -d '\r' < your_private_key > /tmp/key_clean      # strip CRLF if key touched Windows
chmod 600 /tmp/key_clean
ssh-keygen -y -f /tmp/key_clean                      # validate — must print a public key

base64 -w0 /tmp/key_clean > /tmp/key_b64
aws secretsmanager put-secret-value \
  --secret-id de/dev/git-sync-ssh \
  --secret-string file:///tmp/key_b64 \
  --region ap-south-2
```

**ClusterSecretStore (no `auth` block — uses EC2 instance profile via IMDS):**
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: aws-secrets-manager
spec:
  provider:
    aws:
      service: SecretsManager
      region: ap-south-2
  retrySettings:
    maxRetries: 5
    retryInterval: "10s"
```

**ExternalSecret — the critical field is `decodingStrategy: Base64`:**
```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: airflow-git-ssh
  namespace: airflow
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  target:
    name: airflow-git-ssh
    template:
      type: Opaque
      data:
        gitSshKey: "{{ .raw_ssh_key }}"     # hardcoded key name expected by Airflow chart's git-sync mount
  data:
    - secretKey: raw_ssh_key
      remoteRef:
        key: de/dev/git-sync-ssh
        decodingStrategy: Base64            # decodes back to real PEM before templating into the K8s secret
```

Without `decodingStrategy: Base64`, ESO writes the raw base64 *text* into `gitSshKey` — passes casual inspection (no CRLF, looks "clean") but isn't a valid key, so `ssh-keygen`/libcrypto reject it.

### Verification (the only real proof)
```bash
kubectl get secret airflow-git-ssh -n airflow -o jsonpath='{.data.gitSshKey}' | base64 -d > /tmp/check_key
chmod 600 /tmp/check_key
ssh-keygen -y -f /tmp/check_key    # must print ssh-ed25519 AAAA...
```
`cat -A` / CRLF checks alone are **not sufficient** — they can look clean while the content is still wrong (e.g. undecoded base64). Always confirm with `ssh-keygen -y`.

---

## 2. AWS service access for the k3s cluster on EC2 (Instance Profile, not IRSA)

### Design decision
IRSA (IAM Roles for Service Accounts) relies on an EKS-specific pod identity webhook that doesn't exist on self-managed k3s. Rather than building a webhook from scratch, we use the **EC2 Instance Profile** — the node's own IAM role — which every pod on the node inherits by default via the instance metadata service (IMDS), unless overridden.

### Setup
**IAM policy (read-only Secrets Manager, scoped to path):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
      "Resource": "arn:aws:secretsmanager:ap-south-2:940507692220:secret:de/dev/*"
    }
  ]
}
```
Attached to role `EC2-K3s-Secret-Role`, trusted by the EC2 service, then attached to the node via **EC2 → Actions → Security → Modify IAM Role**.

**Critical gotcha:** any static credentials on the node (`~/.aws/credentials`, `AWS_ACCESS_KEY_ID` env vars) silently override the instance profile due to AWS CLI credential precedence. We hit this directly — a leftover `eso-dev-user` static credential caused `aws sts get-caller-identity` to show the wrong identity for hours of debugging. **Fix:** remove any static credentials from the node entirely.
```bash
rm -f ~/.aws/credentials
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
aws sts get-caller-identity   # should show assumed-role/EC2-K3s-Secret-Role/i-xxxx
```

### RBAC — a separate, commonly-missed layer
Instance profile solves **AWS-side** auth. It does *not* solve **Kubernetes-side** RBAC — the ServiceAccount running each pod still needs explicit Role/RoleBinding permissions to create/manage other pods (needed for KubernetesExecutor to launch task pods).

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow-sa
  namespace: airflow

---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-worker-role
  namespace: airflow
rules:
  - apiGroups: [""]
    resources: [pods]
    verbs: [get, list, watch, create, delete, patch]
  - apiGroups: [""]
    resources: [pods/log]
    verbs: [get, list, watch]
  - apiGroups: [""]
    resources: [pods/exec]
    verbs: [get, create]
  - apiGroups: [""]
    resources: [configmaps]
    verbs: [get, list, watch]
  - apiGroups: [""]
    resources: [secrets]
    verbs: [get, list]
  - apiGroups: [""]
    resources: [events]
    verbs: [get, list, watch]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-worker-rolebinding
  namespace: airflow
subjects:
  - kind: ServiceAccount
    name: airflow-sa
    namespace: airflow
  - kind: ServiceAccount
    name: airflow-scheduler        # see Bug #3 below — this was the actual missing piece
    namespace: airflow
roleRef:
  kind: Role
  name: airflow-worker-role
  apiGroup: rbac.authorization.k8s.io
```

---

## 3. Remote logging setup (S3)

### Setup
**Extend the existing instance role** with S3 permissions (same role as Section 2 — one identity, one place to manage):
```bash
aws iam put-role-policy --profile admin \
  --role-name EC2-K3s-Secret-Role \
  --policy-name AirflowS3RemoteLogging \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::de-infra-bucket",
        "arn:aws:s3:::de-infra-bucket/airflow/logs/*"
      ]
    }]
  }'
```

**Airflow connection via env var** — empty auth in the URI triggers boto3's default credential chain (→ instance profile):
```yaml
env:
  - name: AIRFLOW_CONN_AWS_DEFAULT
    value: 'aws://?region_name=ap-south-2'
```

**Logging config:**
```yaml
config:
  logging:
    remote_logging: "true"
    remote_log_conn_id: aws_default
    remote_base_log_folder: s3://de-infra-bucket/airflow/logs
    encrypt_s3_logs: "false"
```

### Verification
```bash
aws s3 ls s3://de-infra-bucket/airflow/logs/ --recursive --region ap-south-2 | tail -10
```
Real proof: task pod logs persist in S3 after the pod itself has already terminated (KubernetesExecutor pods are ephemeral — local logs vanish with the pod).

---

## 4. Bugs / gaps found and fixed

| # | Issue | Root Cause | Fix |
|---|-------|-----------|-----|
| 1 | `git-sync-init`: `Load key "/etc/git-secret/ssh": Is a directory` | Secret volume mounted without `subPath`, so K8s mounted the whole secret dir instead of one file | Use `subPath` matching the secret's data key name, or set `GIT_SYNC_SSH_KEY_FILE` to the exact file path inside the mount |
| 2 | `git-sync-init`: `error in libcrypto` (recurring, ~8 iterations) | Multi-line PEM key corrupted by CRLF and/or cross-machine/clipboard transfer | Base64-encode before storing in Secrets Manager; `decodingStrategy: Base64` on ExternalSecret (see Section 1) |
| 3 | KubernetesExecutor tasks stuck permanently in `queued`, no pods created | `airflow-scheduler` pod runs under ServiceAccount **`airflow-scheduler`** (chart-managed), not `airflow-sa` — the RBAC RoleBinding only granted permissions to `airflow-sa`. `serviceAccount.create: false` + `worker_service_account_name: airflow-sa` in values.yaml only controls the **worker/task pod** SA, not the scheduler's own SA. | Add `airflow-scheduler` as a second `subjects` entry on the existing RoleBinding |
| 4 | `Invalid executor_config` error on every task using `pod_override` | `executor_config["pod_override"]` was a plain Python dict; the `cncf.kubernetes` provider requires an actual `kubernetes.client.V1Pod` object | Rebuild using `from kubernetes.client import models as k8s` and `k8s.V1Pod(spec=k8s.V1PodSpec(...))` instead of raw dict literals |
| 5 | Admin login: correct password rejected as "Invalid login" | Two-layered chart schema mismatch: (a) values.yaml used a **top-level** `defaultUser:` block, but this chart version expects `createUserJob.defaultUser.*`; (b) even once nested correctly, `createUserJob` is a one-shot Helm hook — password changes on `helm upgrade` don't retroactively update an already-existing user | Move to `createUserJob.defaultUser.*`; for existing users, reset directly: `airflow users reset-password -u admin -p '<pw>'` |
| 6 | `ExternalSecret` stuck in `SecretSyncedError` despite YAML "looking correct" | `secretStoreRef.name` typo (`aws-secret-manager` vs `aws-secrets-manager`) present in the actual file on disk, not just a stale in-cluster resource — `kubectl apply` was faithfully applying the typo every time | Fix the typo directly in the source YAML file, not just the live K8s object |
| 7 | `kubectl exec ... -- cat /opt/airflow/...` → `No such file or directory` | Git Bash / MINGW64 auto-translates absolute Unix paths (`/opt/...`) into Windows paths before they reach `kubectl` | Prefix with double slash (`//opt/airflow/...`) to bypass MSYS path conversion |
| 8 | `helm install` CRD errors: invalid ownership metadata / `spec.conversion.strategy` required | Leftover CRDs from a prior partial/differently-versioned ESO install, with mismatched Helm release-namespace annotations and stale `spec.conversion` blocks | Delete stale CRDs (`kubectl delete crd ...`) after confirming no live `ExternalSecret`/`SecretStore` resources depend on them, then reinstall clean |

---

## 5. Troubleshooting command reference

**ESO / ExternalSecret sync status:**
```bash
kubectl get externalsecrets -n airflow
kubectl describe externalsecret <name> -n airflow      # check Status.Conditions.Message for the real error
```

**Verify a synced secret's actual content (not just sync status):**
```bash
kubectl get secret <name> -n airflow -o jsonpath='{.data.<key>}' | base64 -d
```

**Validate an SSH key is genuinely usable (not just clean-looking):**
```bash
kubectl get secret airflow-git-ssh -n airflow -o jsonpath='{.data.gitSshKey}' | base64 -d > /tmp/k
chmod 600 /tmp/k && ssh-keygen -y -f /tmp/k
```

**RBAC — check what a ServiceAccount can actually do:**
```bash
kubectl auth can-i create pods --as=system:serviceaccount:airflow:<sa-name> -n airflow
kubectl get deploy <deployment> -n airflow -o jsonpath='{.spec.template.spec.serviceAccountName}'
```

**git-sync — confirm latest commit synced:**
```bash
kubectl logs -n airflow deploy/airflow-dag-processor -c git-sync --tail=20
```

**Scheduler — task scheduling / pod-launch errors:**
```bash
kubectl logs -n airflow deploy/airflow-scheduler -c scheduler --tail=100
```

**Force a fresh DAG parse (bypass stale bundle cache):**
```bash
kubectl exec -n airflow deploy/airflow-dag-processor -c dag-processor -- airflow dags reserialize
```

**Confirm which secret / CRD apiVersion is actually being served:**
```bash
kubectl get crd <crd-name> -o jsonpath='{range .spec.versions[*]}{.name}{" served="}{.served}{" storage="}{.storage}{"\n"}{end}'
```

**Users:**
```bash
kubectl exec -n airflow deploy/airflow-api-server -- airflow users list
kubectl exec -n airflow deploy/airflow-api-server -- airflow users reset-password -u admin -p '<new-password>'
```

**AWS identity actually in effect on the node (catches static-credential override):**
```bash
aws sts get-caller-identity
```

**S3 remote logging — confirm logs are landing:**
```bash
aws s3 ls s3://de-infra-bucket/airflow/logs/ --recursive --region ap-south-2 | tail -10
```

**Git Bash / MINGW64 path translation workaround:**
```bash
kubectl exec -n airflow <pod> -- cat //opt/airflow/<file>
# or: MSYS_NO_PATHCONV=1 kubectl ...
```