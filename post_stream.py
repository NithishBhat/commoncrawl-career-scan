"""Stream per-file scan outputs (*.jsonl.gz) into Parquet layers, then build summaries.

    python post_stream.py RAW_DIR OUT_DIR patterns.tsv

Layers (OUT_DIR):  matches, homepages, flagged_text, job_pages, file_stats  (.parquet)
Summaries:         domains_lifesci.parquet (one row per domain, best homepage scores)
                   websites_verified.parquet/.csv (best domain per no-website company, with evidence)
                   report.json (yields)
"""
import gzip
import json
import re
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

raw, out, pats = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
out.mkdir(parents=True, exist_ok=True)
META = [l.rstrip("\n").split("\t") for l in open(pats, encoding="utf-8")]

S = pa.string()
SCHEMAS = {
    "m": ("matches", pa.schema([("pid", pa.int32()), ("where", S), ("gated", pa.bool_()), ("url", S), ("dom", S),
                                ("title", S), ("snip", S)])),
    "h": ("homepages", pa.schema([("host", S), ("dom", S), ("url", S), ("title", S), ("ls", pa.int16()), ("sig", S),
                                  ("us", pa.int8()), ("hosp", pa.int8()), ("len", pa.int32()), ("flag", S),
                                  ("a_food", pa.int8()), ("a_pharma", pa.int8()), ("a_cosmetics", pa.int8()),
                                  ("a_devices", pa.int8()), ("a_hospitals", pa.int8())])),
    "x": ("flagged_text", pa.schema([("dom", S), ("url", S), ("text", S)])),
    "s": ("file_stats", pa.schema([("src", S), ("gz_mb", pa.float64()), ("raw_mb", pa.float64()), ("pages", pa.int32()),
                                   ("eng", pa.int32()), ("home", pa.int32()), ("flag", pa.int32()), ("match", pa.int32()),
                                   ("job", pa.int32()), ("fetch_s", pa.float64()), ("cpu_s", pa.float64())])),
}


def convert():
    writers = {t: pq.ParquetWriter(out / f"{name}.parquet", sch, compression="zstd") for t, (name, sch) in SCHEMAS.items()}
    bufs = {t: [] for t in SCHEMAS}

    def flush(t):
        if bufs[t]:
            sch = SCHEMAS[t][1]
            writers[t].write_table(pa.Table.from_pylist(bufs[t], schema=sch))
            bufs[t] = []

    files = sorted(raw.glob("*.jsonl.gz"))
    for i, f in enumerate(files):
        for line in gzip.open(f, "rt", encoding="utf-8"):
            r = json.loads(line)
            t = r.pop("t")
            if t not in bufs:          # 'j' job-page rows are not kept
                continue
            bufs[t].append(r)
            if len(bufs[t]) >= 200000:
                flush(t)
        if i % 2000 == 0:
            print(f"converted {i}/{len(files)}", flush=True)
    for t in SCHEMAS:
        flush(t)
        writers[t].close()
    return len(files)


LS_WORDS = re.compile(r"pharma|biotech|bio|medical|device|diagnost|lab|clinic|therapeut|surgical|health|nutri|"
                      r"supplement|cosmetic|skin|beauty|food|life ?scien|drug|rx|med\b|hospital|fda|gmp", re.I)


SUFFIX_OK = ("inc", "usa", "us", "bio", "biotech", "pharma", "pharmaceuticals", "med", "medical", "labs", "lab",
             "corp", "group", "health", "rx", "llc", "co", "intl", "therapeutics",
             "sciences", "diagnostics", "dx", "devices", "products", "hq",
             "laboratories", "bioscience", "biosciences", "")


sys.path.insert(0, str(Path(__file__).parent))
import scan as _scan  # noqa: E402
GENERIC_FIRST = _scan.COMMON | {w.encode() for w in ("health", "medical", "bio", "pharma", "total", "advanced", "premier",
                                                      "precision", "integrated", "applied", "superior", "quality",
                                                      "allied", "alliance", "united", "standard", "modern", "smart")}


def label_strong(pattern, dom):
    """Domain label is essentially the company name: equal, name + a generic suffix, or a 5+ char prefix of
    the name ('bonesetter' for 'bonesetter holdings'), or the name's initials + generic suffix."""
    label = dom.split(".")[0].replace("-", "")
    words = pattern.split()
    comp = "".join(words)
    if label == comp:
        return True
    if len(words) == 1:            # one-word names ('freshly', 'albright') must equal the label
        return False
    if label.startswith(comp) and label[len(comp):] in SUFFIX_OK:
        return True
    if len(label) >= 5 and comp.startswith(label) and len(label) >= len(words[0]):
        # label = just a generic first word ('health' for 'health wright') is not enough
        return len(label) > len(words[0]) or words[0].encode() not in GENERIC_FIRST
    if len(words) >= 2:
        ini = "".join(w[0] for w in words)
        if len(ini) >= 3 and label.startswith(ini) and label[len(ini):] in SUFFIX_OK:
            return True
    return False


def summarize(nfiles):
    rep = {"files": nfiles}
    st = pq.read_table(out / "file_stats.parquet")
    for c in ("pages", "eng", "home", "gz_mb", "cpu_s", "fetch_s"):
        rep[c] = round(float(pc.sum(st[c]).as_py() or 0), 1)
    rep["cpu_s_per_file"] = round(rep["cpu_s"] / max(nfiles, 1), 2)

    # per-domain life-science table (best homepage per domain)
    h = pq.read_table(out / "homepages.parquet", columns=["dom", "ls", "us", "hosp", "flag", "title", "url",
                                                          "a_food", "a_pharma", "a_cosmetics", "a_devices", "a_hospitals"])
    flagged = pc.not_equal(h["flag"], "")
    h = h.append_column("is_flag", flagged)
    g = h.group_by("dom", use_threads=False).aggregate([("ls", "max"), ("us", "max"), ("hosp", "max"), ("is_flag", "any"),
                                     ("a_food", "max"), ("a_pharma", "max"), ("a_cosmetics", "max"),
                                     ("a_devices", "max"), ("a_hospitals", "max"), ("flag", "distinct"),
                                     ("title", "first"), ("url", "first")])
    g = g.append_column("flags", pa.array([",".join(sorted({x for v in (row or []) for x in (v or "").split(",") if x}))
                                           for row in g["flag_distinct"].to_pylist()]))
    g = g.drop_columns(["flag_distinct"])
    pq.write_table(g, out / "domains_all.parquet", compression="zstd")
    ls_dom = g.filter(pc.and_(g["is_flag_any"], pc.greater_equal(g["us_max"], 1)))
    pq.write_table(ls_dom, out / "domains_lifesci.parquet", compression="zstd")
    rep["homepage_domains"] = g.num_rows
    rep["lifesci_domains_us"] = ls_dom.num_rows
    fl = ls_dom["flags"].to_pylist()
    rep["lifesci_by_area"] = {a: sum(a in f for f in fl) for a in ("ls", "pharma/device", "food", "cosmetics", "hospital")}
    lsd = dict(zip(g["dom"].to_pylist(), zip(g["ls_max"].to_pylist(), g["is_flag_any"].to_pylist(), g["us_max"].to_pylist())))
    del h, g

    # name matches -> best domain per pattern
    m = pq.read_table(out / "matches.parquet", columns=["pid", "where", "gated", "dom"])
    rep["match_rows"] = m.num_rows
    pd_ = m.group_by(["pid", "dom"], use_threads=False).aggregate([("gated", "any"), ("where", "distinct"),
                                                                   ("pid", "count")])
    del m
    per_pid = pd_.group_by("pid").aggregate([("dom", "count_distinct")])
    ndom = dict(zip(per_pid["pid"].to_pylist(), per_pid["dom_count_distinct"].to_pylist()))
    best = {}
    for pid, dom, gated, where, cnt in zip(*(pd_[c].to_pylist() for c in
                                             ("pid", "dom", "gated_any", "where_distinct", "pid_count"))):
        ls, isflag, us = lsd.get(dom, (0, False, 0))
        lsword = bool(LS_WORDS.search(dom))
        amb = ndom.get(pid, 1)
        strong = label_strong(META[pid][0], dom)
        score = (3 if gated else 0) + (4 if strong else 0) + (2 if "title" in where else 0)             + (3 if isflag else 2 if (ls or 0) >= 1 else 0) + (1 if lsword else 0) + (1 if (us or 0) >= 1 else 0)             - (3 if amb > 20 else 1 if amb > 5 else 0)
        if pid not in best or score > best[pid][0]:
            best[pid] = [score, dom, "", "", bool(gated), "title" in where, bool(isflag), ls or 0, us or 0, amb, strong]
    del pd_
    # pass 2: url + title only for the chosen (pid, dom) pairs
    want = {(p, v[1]) for p, v in best.items()}
    pf = pq.ParquetFile(out / "matches.parquet")
    for batch in pf.iter_batches(batch_size=500000, columns=["pid", "dom", "url", "title", "where"]):
        for pid, dom, url, title, where in zip(*(batch.column(c).to_pylist() for c in ("pid", "dom", "url", "title", "where"))):
            if (pid, dom) in want:
                v = best[pid]
                if not v[2] or (where == "title" and not v[3]):
                    v[2], v[3] = url, title or ""
    rows = []
    for pid, (score, dom, url, title, gated, intitle, isflag, ls, us, amb, strong) in best.items():
        lsev = isflag or ls >= 1 or LS_WORDS.search((title or "") + " " + dom) is not None
        tld = dom.rsplit(".", 1)[-1]
        us_tld = len(tld) != 2 or tld in ("us", "co", "io", "ai", "md")
        verified = strong and us_tld and amb <= 5 and (intitle or isflag or ls >= 1)
        if verified and " " not in META[pid][0]:      # one-word names: common TLD + life-science evidence
            verified = tld in ("com", "net", "us", "bio", "health", "co") and \
                (isflag or ls >= 1 or LS_WORDS.search(title or "") is not None)
        tier = "verified" if verified else "candidate" if (gated and amb <= 20 and lsev) else "weak"
        rows.append({"pattern": META[pid][0], "kind": META[pid][1], "companies": META[pid][2], "domain": dom,
                     "url": url, "title": title, "score": score, "tier": tier, "verified": bool(verified),
                     "label_strong": strong, "gated": gated,
                     "in_title": intitle, "domain_lifesci_flag": isflag, "domain_ls": ls, "domain_us": us,
                     "pattern_domains": amb})
    v = pa.Table.from_pylist(rows)
    pq.write_table(v, out / "websites_best.parquet", compression="zstd")
    ver = v.filter(v["verified"])
    import csv
    with open(out / "websites_verified.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["companies", "pattern", "domain", "url", "title", "score"])
        for r in sorted(ver.to_pylist(), key=lambda r: -r["score"]):
            w.writerow([r["companies"], r["pattern"], r["domain"], r["url"], r["title"], r["score"]])
    comp = {c for r in ver.to_pylist() for c in r["companies"].split(" || ")}
    cand = {c for r in rows if r["tier"] == "candidate" for c in r["companies"].split(" || ")} - comp
    allc = {c for r in rows for c in r["companies"].split(" || ")}
    rep.update(patterns_hit=len(best), companies_any_match=len(allc), verified_patterns=ver.num_rows,
               verified_companies=len(comp), verified_domains=len(set(ver["domain"].to_pylist())),
               candidate_companies=len(cand))

    (out / "report.json").write_text(json.dumps(rep, indent=1))
    print(json.dumps(rep, indent=1))


if __name__ == "__main__":
    n = convert() if "--summary-only" not in sys.argv else len(list(raw.glob("*.jsonl.gz")))
    summarize(n)
