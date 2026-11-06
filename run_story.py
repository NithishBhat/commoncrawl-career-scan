"""Collect the 'story' of the AWS run: CloudWatch per-instance metrics + S3 completion timeline.

    python run_story.py STORY_DIR RUN_PREFIX [RUN_PREFIX ...]

Reads instance IDs from cloudtrail_runinstances.json (saved earlier), queries CloudWatch (AWS/EC2 CPUUtilization,
NetworkIn, NetworkOut, 5-min) via the AWS CLI, and lists s3://$CCSCAN_BUCKET/out/<run>/ LastModified times.
Writes: metrics.json, timeline_<run>.json (files completed per minute), summary.json
"""
import collections
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

story = Path(sys.argv[1])
runs = sys.argv[2:]
env = dict(os.environ, AWS_PAGER="", MSYS_NO_PATHCONV="1")


def aws(*args):
    r = subprocess.run(["aws", *args, "--output", "json"], capture_output=True, text=True, env=env, shell=False)
    if r.returncode:
        raise RuntimeError(r.stderr[:500])
    return json.loads(r.stdout or "{}")


inst = []
ev = aws("cloudtrail", "lookup-events", "--lookup-attributes", "AttributeKey=EventName,AttributeValue=RunInstances",
         "--start-time", "2026-11-05T14:00:00Z")
(story / "cloudtrail_runinstances.json").write_text(json.dumps(ev, default=str))
for e in ev["Events"]:
    ct = json.loads(e["CloudTrailEvent"])
    for it in ((ct.get("responseElements") or {}).get("instancesSet") or {}).get("items", []):
        tags = {t["key"]: t["value"] for t in ((it.get("tagSet") or {}).get("items") or [])}
        inst.append({"id": it["instanceId"], "launch": e["EventTime"] if isinstance(e["EventTime"], str) else str(e["EventTime"]),
                     "type": it.get("instanceType"), "market": it.get("instanceLifecycle") or "on-demand",
                     "name": tags.get("Name", "")})
start = min(datetime.fromisoformat(i["launch"]) for i in inst).astimezone(timezone.utc) - timedelta(minutes=5)
end = datetime.now(timezone.utc)

queries = []
for k, i in enumerate(inst):
    for m in ("CPUUtilization", "NetworkIn", "NetworkOut"):
        queries.append({"Id": f"q{k}_{m[:4].lower()}{m[-3:].lower()}", "Label": f"{i['id']}|{m}",
                        "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": m,
                                                  "Dimensions": [{"Name": "InstanceId", "Value": i["id"]}]},
                                       "Period": 300, "Stat": "Average" if m == "CPUUtilization" else "Sum"}})
series = {}
for chunk in range(0, len(queries), 400):
    q = queries[chunk:chunk + 400]
    (story / "_q.json").write_text(json.dumps(q))
    token = None
    while True:
        args = ["cloudwatch", "get-metric-data", "--metric-data-queries", f"file://{(story / '_q.json').as_posix()}",
                "--start-time", start.isoformat(), "--end-time", end.isoformat()]
        if token:
            args += ["--next-token", token]
        d = aws(*args)
        for r in d.get("MetricDataResults", []):
            s = series.setdefault(r["Label"], {})
            s.update(dict(zip(r["Timestamps"], r["Values"])))
        token = d.get("NextToken")
        if not token:
            break
(story / "_q.json").unlink(missing_ok=True)
(story / "metrics.json").write_text(json.dumps({"instances": inst, "series": series}, indent=0))

summary = {"instances": len(inst), "spot": sum(i["market"] == "spot" for i in inst),
           "on_demand": sum(i["market"] != "spot" for i in inst), "window_utc": [start.isoformat(), end.isoformat()]}
for run in runs:
    times = []
    token = None
    while True:
        args = ["s3api", "list-objects-v2", "--bucket", os.environ["CCSCAN_BUCKET"], "--prefix", f"out/{run}/",
                "--query", "{c: Contents[].[LastModified,Size], t: NextContinuationToken}"]
        if token:
            args += ["--starting-token", token]
        d = aws(*args)
        times += d.get("c") or []
        token = d.get("t")
        if not token:
            break
    per_min = collections.Counter(t[:16] for t, _ in times)
    (story / f"timeline_{run}.json").write_text(json.dumps(sorted(per_min.items())))
    if times:
        ts = sorted(t for t, _ in times)
        summary[run] = {"files": len(times), "first": ts[0], "last": ts[-1], "out_bytes": sum(s for _, s in times),
                        "peak_files_per_min": max(per_min.values())}
(story / "summary.json").write_text(json.dumps(summary, indent=1))
print(json.dumps(summary, indent=1))
print("series:", len(series))
