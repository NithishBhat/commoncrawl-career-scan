#!/bin/bash
# Common Crawl scan on AWS (us-east-1).   Usage:
#   ./aws_run.sh setup              # bucket + instance role + upload code/lists (one time)
#   ./aws_run.sh test               # 1 x c7a.2xlarge spot: 10-file check, then 1,000 files  (~$0.10-0.20)
#   ./aws_run.sh full               # 1 x c7a.8xlarge spot (32 vCPU) + 1 x c7a.4xlarge on-demand (16 vCPU)
#   ./aws_run.sh status RUN         # files done for run (test10 | test1k | full)
#   ./aws_run.sh cleanup            # terminate ccscan instances, empty+delete bucket, delete role
# Safety: every instance runs `shutdown -h +240` first and has InstanceInitiatedShutdownBehavior=terminate,
# so it terminates on finish or after 4 h at the latest. Root EBS volume is deleted on termination.
set -euo pipefail
export AWS_PAGER="" AWS_DEFAULT_REGION=us-east-1 MSYS_NO_PATHCONV=1
HERE="$(cd "$(dirname "$0")" && pwd)"
B=${CCSCAN_BUCKET:-ccscan-$(aws sts get-caller-identity --query Account --output text)}
CRAWL=CC-MAIN-2026-43

ud() {  # $1 = run name, $2 = list file in bucket, $3 = minutes budget for new files
cat <<EOF
#!/bin/bash
shutdown -h 13:00   # single backstop: 13:00 UTC, before credits expire 14:22 UTC
exec > /var/log/ccscan.log 2>&1
set -x
IID=\$(cat /var/lib/cloud/data/instance-id)
( while sleep 120; do aws s3 cp /var/log/ccscan.log s3://$B/logs/$1-\$IID.log --only-show-errors; done ) &
dnf install -y python3.11 python3.11-pip || dnf install -y python3-pip
PY=\$(command -v python3.11 || command -v python3)
\$PY -m pip install -q boto3
mkdir -p /opt/ccscan && cd /opt/ccscan
aws s3 cp s3://$B/code/ . --recursive
aws s3 cp s3://$B/lists/$2 list.txt
ALL=""; aws s3 cp s3://$B/lists/$1-all.txt all.txt && ALL="--all all.txt"
\$PY scan.py --patterns patterns.tsv --rules rules.json --list list.txt \$ALL --out s3://$B/out/$1 --minutes $3
aws s3 cp /var/log/ccscan.log s3://$B/logs/$1-\$IID.log
shutdown -h now
EOF
}

launch() {  # $1 type, $2 market (spot|ondemand), $3 run, $4 list, $5 minutes
  AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text)
  MKT=(); [ "$2" = spot ] && MKT=(--instance-market-options 'MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}')
  aws ec2 run-instances --image-id "$AMI" --instance-type "$1" "${MKT[@]}" \
    --iam-instance-profile Name=ccscan-ec2 --instance-initiated-shutdown-behavior terminate \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=16,VolumeType=gp3,DeleteOnTermination=true}' \
    --user-data "$(ud "$3" "$4" "$5")" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ccscan-$3},{Key=project,Value=ccscan}]" \
    --query 'Instances[0].InstanceId' --output text
}

case "${1:-}" in
setup)
  aws s3api head-bucket --bucket $B 2>/dev/null || aws s3api create-bucket --bucket $B >/dev/null
  aws s3api put-public-access-block --bucket $B --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws iam get-role --role-name ccscan-ec2 >/dev/null 2>&1 || aws iam create-role --role-name ccscan-ec2 \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam put-role-policy --role-name ccscan-ec2 --policy-name ccscan-s3 --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
    {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:ListBucket\"],\"Resource\":[\"arn:aws:s3:::commoncrawl\",\"arn:aws:s3:::commoncrawl/*\"]},
    {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:ListBucket\"],\"Resource\":[\"arn:aws:s3:::$B\",\"arn:aws:s3:::$B/*\"]}]}"
  aws iam get-instance-profile --instance-profile-name ccscan-ec2 >/dev/null 2>&1 || {
    aws iam create-instance-profile --instance-profile-name ccscan-ec2 >/dev/null
    aws iam add-role-to-instance-profile --instance-profile-name ccscan-ec2 --role-name ccscan-ec2; sleep 15; }
  aws s3 cp "$HERE/scan.py" s3://$B/code/scan.py
  aws s3 cp "$HERE/patterns.tsv" s3://$B/code/patterns.tsv
  aws s3 cp "$HERE/rules.json" s3://$B/code/rules.json
  aws s3 cp "s3://commoncrawl/crawl-data/$CRAWL/wet.paths.gz" - | gzip -dc > "$HERE/wet.paths"
  awk 'NR%10000==4321' "$HERE/wet.paths" > "$HERE/test10.txt"
  { cat "$HERE/test10.txt"; awk 'NR%100==7' "$HERE/wet.paths" | head -990; } > "$HERE/test1k.txt"
  awk 'NR%3!=0' "$HERE/wet.paths" > "$HERE/full-a.txt"      # 2/3 -> 32 vCPU spot
  awk 'NR%3==0' "$HERE/wet.paths" > "$HERE/full-b.txt"      # 1/3 -> 16 vCPU on-demand
  for f in test10 test1k full-a full-b; do aws s3 cp "$HERE/$f.txt" s3://$B/lists/$f.txt; done
  aws s3 cp "$HERE/wet.paths" s3://$B/lists/full-all.txt
  echo setup done ;;
test)
  # 1,000 files (list starts with the 10-file sample, so the first outputs are the 10-file check); 90 min cap
  id=$(launch c7i-flex.large spot test1k test1k.txt 90 || launch c7i-flex.large ondemand test1k test1k.txt 90); echo "test instance: $id" ;;
full)
  # Free plan: 2-vCPU types only. 24 shards: 16 spot (32 vCPU quota) + 8 on-demand (16 vCPU quota).
  # ${2:-210} = minutes budget for starting new files (instance hard cap is 240).
  if ! aws s3 ls s3://$B/lists/full-23.txt >/dev/null 2>&1; then
    awk '{ f = sprintf("'"$HERE"'/full-%02d.txt", (NR-1) % 24); print > f }' "$HERE/wet.paths"
    for i in $(seq -w 0 23); do aws s3 cp "$(cygpath -w "$HERE/full-$i.txt")" s3://$B/lists/full-$i.txt --only-show-errors; done
  fi
  for i in $(seq -w 0 23); do
    if [ "$((10#$i))" -lt 16 ]; then
      id=$(launch c7i-flex.large spot full full-$i.txt "${2:-210}" 2>/dev/null || launch c7i-flex.large ondemand full full-$i.txt "${2:-210}" 2>&1 | tail -1)
    else
      id=$(launch c7i-flex.large ondemand full full-$i.txt "${2:-210}" 2>&1 | tail -1)
    fi
    echo "shard $i: $id"
  done ;;
watlists)
  aws s3 cp "s3://commoncrawl/crawl-data/$CRAWL/wat.paths.gz" - | gzip -dc > "$HERE/wat.paths"
  awk 'NR%10000==4321' "$HERE/wat.paths" > "$HERE/wattest.txt"
  awk '{ f = sprintf("'"$HERE"'/wat-%02d.txt", (NR-1) % 24); print > f }' "$HERE/wat.paths"
  for f in wattest $(seq -f "wat-%02g" 0 23); do aws s3 cp "$(cygpath -w "$HERE/$f.txt")" s3://$B/lists/$f.txt --only-show-errors; done
  aws s3 cp "$(cygpath -w "$HERE/wat.paths")" s3://$B/lists/wat-all.txt --only-show-errors
  wc -l "$HERE/wat.paths" ;;
wattest)
  id=$(launch c7i-flex.large spot wattest wattest.txt 30 || launch c7i-flex.large ondemand wattest wattest.txt 30); echo "wat test: $id" ;;
wat)
  for i in $(seq -w 0 23); do
    if [ "$((10#$i))" -lt 16 ]; then
      id=$(launch c7i-flex.large spot wat wat-$i.txt "${2:-210}" 2>/dev/null || launch c7i-flex.large ondemand wat wat-$i.txt "${2:-210}" 2>&1 | tail -1)
    else
      id=$(launch c7i-flex.large ondemand wat wat-$i.txt "${2:-210}" 2>&1 | tail -1)
    fi
    echo "shard $i: $id"
  done ;;
status)
  n=$(aws s3 ls s3://$B/out/$2/ | wc -l); echo "$2: $n files done"
  aws ec2 describe-instances --filters Name=tag:project,Values=ccscan Name=instance-state-name,Values=pending,running,shutting-down \
    --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name,LaunchTime]' --output text ;;
cleanup)
  ids=$(aws ec2 describe-instances --filters Name=tag:project,Values=ccscan Name=instance-state-name,Values=pending,running,stopped \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  [ -n "$ids" ] && aws ec2 terminate-instances --instance-ids $ids >/dev/null
  aws s3 rm s3://$B --recursive --quiet || true
  aws s3api delete-bucket --bucket $B || true
  aws iam remove-role-from-instance-profile --instance-profile-name ccscan-ec2 --role-name ccscan-ec2 || true
  aws iam delete-instance-profile --instance-profile-name ccscan-ec2 || true
  aws iam delete-role-policy --role-name ccscan-ec2 --policy-name ccscan-s3 || true
  aws iam delete-role --role-name ccscan-ec2 || true
  echo "left: instances=$(aws ec2 describe-instances --filters Name=tag:project,Values=ccscan Name=instance-state-name,Values=pending,running \
    --query 'length(Reservations[].Instances[])') volumes=$(aws ec2 describe-volumes --filters Name=status,Values=available --query 'length(Volumes)')" ;;
*) sed -n 2,9p "$0" ;;
esac
