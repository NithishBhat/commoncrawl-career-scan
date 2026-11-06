"""Tech + automotive companies with links to their career sites (no job postings).

    python tech_companies.py OUT_DIR RAW_DIR [RAW_DIR ...]

Input rows from the WAT pass: "i" (homepage industry tags), "a" (ATS board links), "c" (careers links/subdomains).
Output (OUT_DIR):
  companies_tech.csv, companies_automotive.csv  — company, domain, sector terms, career_url, ats_board, system
  companies_all.parquet, report.json
"""
import collections
import csv
import gzip
import json
import re
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

out, raws = Path(sys.argv[1]), [Path(p) for p in sys.argv[2:]]
out.mkdir(parents=True, exist_ok=True)
SYS = [("myworkdayjobs", "workday"), ("myworkdaysite", "workday"), ("greenhouse", "greenhouse"), ("lever.co", "lever"),
       ("icims", "icims"), ("ashbyhq", "ashby"), ("smartrecruiters", "smartrecruiters"), ("bamboohr", "bamboohr"),
       ("jobvite", "jobvite"), ("workable", "workable"), ("recruitee", "recruitee"), ("paylocity", "paylocity"),
       ("adp.com", "adp"), ("oraclecloud", "oracle"), ("successfactors", "successfactors"), ("ultipro", "ukg"),
       ("ukg.net", "ukg"), ("applytojob", "jazzhr"), ("jazzhr", "jazzhr"), ("rippling", "rippling"), ("taleo", "taleo"),
       ("breezy", "breezy"), ("dayforce", "dayforce"), ("avature", "avature"), ("eightfold", "eightfold"),
       ("phenompeople", "phenom"), ("paycom", "paycom"), ("paycor", "paycor"), ("brassring", "brassring"),
       ("csod.com", "cornerstone"), ("teamtailor", "teamtailor"), ("pinpointhq", "pinpoint")]
SHARED = re.compile(r"^(cdn|content|assets|static|js|widgets?|api)\.|^(www\.)?(phenompeople|eightfold)\.")


def system_of(host):
    return next((v for k, v in SYS if k in host), "other")


ind = {}
board_doms = collections.defaultdict(collections.Counter)
board_link = {}
dom_boards = collections.defaultdict(set)
careers = collections.defaultdict(set)
nfiles = 0
for f in sorted(x for raw in raws for x in raw.glob("*.jsonl.gz")):
    nfiles += 1
    for line in gzip.open(f, "rt", encoding="utf-8"):
        r = json.loads(line)
        t = r["t"]
        if t == "i":
            e = ind.setdefault(r["dom"], {"title": r["title"], "desc": r["desc"], "url": r["url"], "tech": set(),
                                          "auto": set()})
            e["tech"].update(x for x in r["tech"].split(",") if x)
            e["auto"].update(x for x in r["auto"].split(",") if x)
        elif t == "a" and not r["self"]:
            b = r["board"].lower()
            host = b.split("/")[0]
            if SHARED.search(host):
                b = f"{system_of(host)}@{r['dom']}"
            board_doms[b][r["dom"]] += 1
            board_link.setdefault(b, r["link"])
        elif t == "c" and len(careers[r["dom"]]) < 6:
            careers[r["dom"]].add(r["link"])

# board -> owning domain: slug/label overlap, a script on the domain, or the board's only linking domain
for b, doms in board_doms.items():
    if "@" in b:
        dom_boards[b.split("@", 1)[1]].add(b)
        continue
    slug = re.sub(r"[^a-z0-9]", "", b.split("/", 1)[-1] if "/" in b else b.split(".")[0])
    own = next((d for d, _ in doms.most_common(20)
                if len(d.split(".")[0]) >= 3 and (d.split(".")[0].replace("-", "") in slug or
                                                  (len(slug) >= 4 and slug in d.split(".")[0].replace("-", "")))), None)
    if own is None and len(doms) == 1:
        own = next(iter(doms))
    if own:
        dom_boards[own].add(b)


def name_of(title, dom):
    parts = [p.strip() for p in re.split(r"\s[|\-–—:]\s|\s·\s", title or "") if p.strip()]
    lab = dom.split(".")[0].replace("-", "")
    for p in parts:                       # prefer the title segment that looks like the domain
        if lab[:4] in re.sub(r"[^a-z0-9]", "", p.lower()):
            return p[:80]
    return (parts[-1] if parts else dom)[:80]


rows = []
for d, e in ind.items():
    tech, auto = e["tech"], e["auto"]
    sector = "automotive" if auto else "tech"
    if sector == "tech" and len(tech) < 2:
        continue
    boards = sorted(dom_boards.get(d, ()))
    cl = sorted(careers.get(d, ()), key=lambda u: (("career" not in u and "jobs" not in u), len(u)))
    if not boards and not cl:
        continue                          # only companies with a career link
    tld = d.rsplit(".", 1)[-1]
    rows.append({"company": name_of(e["title"], d), "domain": d, "sector": sector + ("+tech" if auto and len(tech) >= 2 else ""),
                 "tech_terms": ",".join(sorted(tech)), "auto_terms": ",".join(sorted(auto)),
                 "career_url": board_link.get(boards[0], "") if boards else cl[0],
                 "ats_board": boards[0] if boards else "", "ats_system": system_of(boards[0]) if boards else "own-site",
                 "other_boards": ";".join(boards[1:4]), "homepage": e["url"], "description": e["desc"][:250],
                 "us_like_tld": len(tld) != 2 or tld in ("us", "io", "ai", "co")})
pq.write_table(pa.Table.from_pylist(rows), out / "companies_all.parquet", compression="zstd")
cols = ["company", "domain", "sector", "career_url", "ats_board", "ats_system", "other_boards", "tech_terms",
        "auto_terms", "us_like_tld", "homepage", "description"]
for name, keep in (("companies_tech.csv", lambda r: r["sector"] == "tech"),
                   ("companies_automotive.csv", lambda r: r["sector"].startswith("automotive"))):
    sel = sorted((r for r in rows if keep(r)), key=lambda r: (not r["us_like_tld"], r["ats_system"] == "own-site", r["domain"]))
    with open(out / name, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sel)
rep = {"wat_files": nfiles, "homepages_tagged": len(ind), "companies_with_career_link": len(rows),
       "tech": sum(r["sector"] == "tech" for r in rows), "automotive": sum(r["sector"].startswith("automotive") for r in rows),
       "with_ats_board": sum(bool(r["ats_board"]) for r in rows), "us_like_tld": sum(r["us_like_tld"] for r in rows),
       "by_system": dict(collections.Counter(r["ats_system"] for r in rows).most_common(20))}
(out / "report.json").write_text(json.dumps(rep, indent=1))
print(json.dumps(rep, indent=1))
