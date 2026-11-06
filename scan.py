"""One-pass Common Crawl WET scan: (a) company-name matches, (b) life-science homepage scoring.

    python scan.py --patterns patterns.tsv --list paths.txt --out OUTDIR|s3://bucket/prefix [--procs N]

Each list line is a WET source: local file, https URL, or a 'crawl-data/...' key (read from s3://commoncrawl).
One output per WET file: <basename>.jsonl.gz with lines
  {"t":"m", ...}  name match     {"t":"h", ...}  homepage row     {"t":"x", ...}  flagged homepage text
  {"t":"s", ...}  per-file stats (last line)
Existing outputs are skipped, so reruns resume. Python 3.9+, stdlib only (+ boto3 for S3).
"""
import argparse
import gzip
import io
import json
import os
import re
import sys
import time
import urllib.request
from multiprocessing import Pool
from urllib.parse import urlsplit

# --- normalisation identical in spirit to src.common.normalize_company: lower, & -> and, non [a-z0-9] -> space
TABLE = bytes((c if 48 <= c <= 57 or 97 <= c <= 122 else c + 32 if 65 <= c <= 90 else 32) for c in range(256))

LS_STEMS = (b"fda", b"gmp", b"cgmp", b"13485", b"510", b"pharma", b"biotech", b"biopharm", b"medical", b"clinical",
            b"laborator", b"diagnostic", b"supplement", b"nutraceutical", b"cosmetic", b"biolog", b"drug",
            b"therapeut", b"vaccine", b"assay", b"reagent", b"surgical", b"implant", b"ndc", b"clia", b"api",
            b"manufactur", b"oncology", b"molecule", b"antibod", b"sterile", b"orthop", b"dental")

# homepage signals (regex on lowercased raw bytes); each distinct hit = 1 point. 'health' alone never counts.
LS_SIGNALS = {
    "fda": rb"\bfda\b|food and drug administration",
    "gmp": rb"\bc?gmp\b|good manufacturing practice",
    "iso13485": rb"iso\s*13485",
    "510k": rb"\b510\s*\(?k\)?",
    "pharma": rb"\bpharmaceutical|\bbiopharma",
    "biotech": rb"\bbiotech",
    "device": rb"medical (?:device|equipment|instrument)|\bin vitro diagnostic|\bivd\b",
    "diagnostics": rb"\bdiagnostic (?:test|kit|assay)s?|\bassays?\b",
    "lab": rb"clinical laborator|\bclia\b|\bcap[- ]accredited|laboratory services|testing laboratory",
    "clinical": rb"clinical (?:trial|stage|development|research)",
    "pipeline": rb"\bpipeline\b.{0,80}(?:phase|preclinical|candidate)|\bphase (?:1|2|3|i|ii|iii)\b",
    "cdmo": rb"\bcdmo\b|\bcmo\b|contract (?:development|manufacturing|research)|\bcro\b",
    "api": rb"active pharmaceutical ingredient|\bapis?\b.{0,40}(?:manufactur|intermediat)",
    "supplement": rb"dietary supplement|\bnutraceutical|\bvitamins?\b.{0,60}supplement",
    "cosmetic": rb"\bcosmetics?\b|\bskin ?care\b.{0,60}(?:formula|manufactur|product)",
    "biologics": rb"\bbiologics?\b|\bvaccines?\b|monoclonal|\bantibod",
    "drug": rb"\bdrug (?:product|substance|development|delivery)|\bgeneric drugs?\b|\bndc\b|\bover[- ]the[- ]counter\b|\botc\b",
    "regulatory": rb"regulatory (?:affairs|compliance|submission)|\bquality (?:assurance|system)",
    "lifesci": rb"\blife sciences?\b",
    "manufacturer": rb"\bmanufacturer\b|\bmanufacturing facilit",
    "sterile": rb"\bsterile\b|\bsterilization\b",
    "surgical": rb"\bsurgical\b|\borthop(?:a)?edic|\bimplants?\b",
}
LS_RX = {k: re.compile(v) for k, v in LS_SIGNALS.items()}
HOSP_RX = re.compile(rb"\bhospital\b|\bmedical center\b|\bhealth system\b")
STATES = (b"AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|"
          b"ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|PR")
ZIP_RX = re.compile(rb"\b(?:" + STATES + rb")\.?,?\s+\d{5}(?:-\d{4})?\b")
PHONE_RX = re.compile(rb"(?:\+1[\s.-]?)?\(?\b[2-9]\d{2}\)?[\s.-]\d{3}[\s.-]\d{4}\b")
USA_RX = re.compile(rb"\bunited states\b|\busa\b|\bu\.s\.a?\.?")
HOME_RX = re.compile(r"^/?(?:(?:index|default|home)(?:\.\w{2,5})?|en|en-us|en_us|us|en/home)?/?$", re.I)
SL2 = {"co", "com", "org", "net", "ac", "gov", "edu", "or", "ne", "go", "gob", "nhs"}

# --- role families / product areas (rules.json is exported from the pipeline: board_scan.RELEVANT/NOT_RELEVANT,
#     profile.json product_area_keywords), so results line up with the pipeline's filters.
RELEVANT = NOT_RELEVANT = None
AREA_RX = {}
FAMILIES = [
    ("writer", r"medical writer|regulatory writer|technical writer"),
    ("drug safety/PV", r"drug safety|pharmacovigilance|case processing|vigilance"),
    ("complaints/post-market", r"complaint|post.?market"),
    ("clinical trial docs", r"clinical trial|clinical research|trial master|\be?tmf\b|study start.?up|site activation"),
    ("IRB/research compliance", r"\birb\b|institutional review|research compliance|human research"),
    ("RA", r"regulatory|\budi\b|product stewardship|submissions"),
    ("labeling/artwork", r"label+ing|artwork"),
    ("food safety/FSQA", r"food safety|\bfsqa\b|\bsqf\b|haccp"),
    ("microbiology/sterility/EM", r"microbiolog|steril|environmental monitoring|aseptic"),
    ("validation/CSV", r"validation|\bcsv\b"),
    ("supplier quality/auditor", r"supplier quality|auditor"),
    ("batch record/QA ops", r"batch (?:record|release)"),
    ("quality engineer", r"quality engineer"),
    ("QC/lab", r"quality control|\bqc\b|lab(?:oratory)? (?:technician|analyst)"),
    ("QA", r"quality|\bqa\b|document control|\bcapa\b|change control|\bgmp\b|compliance"),
]
FAMILIES = [(f, re.compile(r, re.I)) for f, r in FAMILIES]
ATS_HOSTS = ("myworkdayjobs.com", "myworkdaysite.com", "greenhouse.io", "lever.co", "icims.com", "ashbyhq.com",
             "smartrecruiters.com", "bamboohr.com", "jobvite.com", "workable.com", "recruitee.com", "paylocity.com",
             "workforcenow.adp.com", "oraclecloud.com", "successfactors.com", "successfactors.eu", "ultipro.com",
             "ukg.net", "applytojob.com", "rippling.com", "taleo.net", "jazzhr.com", "breezy.hr", "dayforcehcm.com",
             "avature.net", "eightfold.ai", "phenompeople.com", "paycomonline.net", "paycor.com", "clearcompany.com",
             "teamtailor.com", "pinpointhq.com", "isolvedhire.com", "hrmdirect.com", "careerplug.com", "applicantpro.com",
             "brassring.com", "silkroad.com", "hirebridge.com", "csod.com", "workdayjobs.com")
JOB_PATH_RX = re.compile(r"/(?:jobs?|careers?|positions?|openings?|vacanc|requisition|postings?|job-?details?|"
                         r"employment|opportunit)", re.I)
FOOD_STRONG = re.compile(rb"nutraceutical|dietary supplement|infant formula|\bfsma\b|haccp|\bsqf\b|ingredient statement|"
                         rb"co-?pack|food (?:manufactur|process|ingredient)|\bgfsi\b|\bbrcgs?\b")
COSM_STRONG = re.compile(rb"\bmocra\b|private label|contract manufactur|formulat|\bgmp\b|\bfda\b|manufacturer")


def load_rules(path):
    global RELEVANT, NOT_RELEVANT
    r = json.load(open(path, encoding="utf-8"))
    RELEVANT, NOT_RELEVANT = re.compile(r["relevant"], re.I), re.compile(r["not_relevant"], re.I)
    for area, words in r["areas"].items():
        AREA_RX[area] = [re.compile(rb"\b" + re.escape(w.lower().encode()) + rb"(?![a-z])") for w in words]


def family(title):
    for f, rx in FAMILIES:
        if rx.search(title):
            return f
    return "other"


PAT = {}       # tuple(tokens) -> pattern id
FIRST = {}     # first token -> set of lengths
META = []      # pid -> (pattern, kind, names)


def load_patterns(path):
    for line in open(path, encoding="utf-8"):
        p, kind, names = line.rstrip("\n").split("\t")
        t = tuple(p.encode().split())
        PAT[t] = len(META)
        META.append((p, kind, names))
        pid = PAT[t]
        FIRST.setdefault(t[0], set()).add(len(t))
        distinct = not all(w in COMMON for w in t)
        DISTINCT.append(distinct)
        NEEDLE.append(b" " + b" ".join(t) + b" ")
        if not distinct:
            continue
        comp = b"".join(t)
        if len(comp) >= 5:
            COMPACT.setdefault(comp, []).append(pid)
            for k in range(5, len(comp)):
                PREFIX.setdefault(comp[:k], []).append(pid)
        if len(t) > 1 and len(t[0]) >= 5 and t[0] not in COMMON:
            FIRSTW.setdefault(t[0], []).append(pid)


COMPACT, PREFIX, FIRSTW, DISTINCT, NEEDLE = {}, {}, {}, [], []
_GATE = {}


def gate(label):
    """Pattern ids whose name relates to a domain label: compact name inside the label ('bonesetter' in
    'bonesetterholdings'), label is a prefix of the name, or the name's distinctive first word is in the label."""
    g = _GATE.get(label)
    if g is None:
        g = set()
        n = len(label)
        for i in range(n):
            for j in range(i + 5, n + 1):
                s = label[i:j]
                g.update(COMPACT.get(s, ()))
                g.update(FIRSTW.get(s, ()))
        if n >= 5:
            g.update(PREFIX.get(label, ()))
        if len(g) > 60:
            g = set(sorted(g)[:60])
        if len(_GATE) > 200000:
            _GATE.clear()
        _GATE[label] = g
    return g


def find(tokens, min_len=1):
    """Yield (i, pid) for every pattern occurrence in the token list."""
    for i, tok in enumerate(tokens):
        lens = FIRST.get(tok)
        if lens:
            for L in lens:
                if L >= min_len:
                    pid = PAT.get(tuple(tokens[i:i + L]))
                    if pid is not None:
                        yield i, pid


BIGRAM = {}    # (tok0, tok1) -> [(tuple(tokens), pid)] for multi-word patterns
# Names made only of common English words ('could you', 'one life', 'new england') are title-only.
COMMON = set(b"""a about above after again against all also am an and any are as at back be because been before being
below best between both but by can care center centre city come could day did do does doing down during each even
every first five for four from further get go good great group had has have having he health her here hers herself
him himself his home how i if in into is it its itself just know last life like little live long love made make man
many may me medical more most much my myself name national new next no nor not now of off old on once one only or
other our ours ourselves out over own part people place plus pro quality right same see she should so some such
take than that the their theirs them themselves then there these they this those three through time to today too top
two under united until up us use very was way we well were what when where which while who whom why will with
work world would year you your yours yourself yourselves america american north south east west new york texas
california florida general global first pure natural care family life science sciences research total better""".split())


def find_body(tokens):
    """Multi-word patterns only. Set intersection over the page's bigrams runs in C; only hits get a Python loop."""
    cand = BIGRAM.keys() & set(zip(tokens, tokens[1:])) if len(tokens) > 1 else ()
    for bg in cand:
        a, b = bg
        i = -1
        while True:
            try:
                i = tokens.index(a, i + 1)
            except ValueError:
                break
            if i + 1 < len(tokens) and tokens[i + 1] == b:
                for pat, pid in BIGRAM[bg]:
                    if len(pat) == 2 or tuple(tokens[i:i + len(pat)]) == pat:
                        yield i, pid


def reg_domain(host):
    parts = host.split(".")
    if len(parts) >= 3 and len(parts[-1]) == 2 and parts[-2] in SL2:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def ls_near(tokens, i, w=40):
    for t in tokens[max(0, i - w): i + w]:
        if t.startswith(LS_STEMS):
            return True
    return False


def records(data):
    """Yield (headers dict, content bytes) for each WARC record in decompressed WET data."""
    pos, n = 0, len(data)
    while pos < n:
        h_end = data.find(b"\r\n\r\n", pos)
        if h_end < 0:
            return
        hdr = {}
        for line in data[pos:h_end].split(b"\r\n")[1:]:
            k, _, v = line.partition(b":")
            hdr[k.strip().lower()] = v.strip()
        clen = int(hdr.get(b"content-length", 0))
        start = h_end + 4
        yield hdr, data[start:start + clen]
        pos = start + clen
        while pos < n and data[pos] in (13, 10):
            pos += 1


def fetch(src):
    if os.path.exists(src):
        return open(src, "rb").read()
    if src.startswith("http"):
        req = urllib.request.Request(src, headers={"User-Agent": "ccscan-research/1.0"})
        return urllib.request.urlopen(req, timeout=300).read()
    import boto3
    for attempt in range(5):
        try:
            return boto3.client("s3").get_object(Bucket="commoncrawl", Key=src)["Body"].read()
        except Exception:
            if attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))


def scan_bytes(gz):
    st = {"gz_mb": len(gz) / 1e6, "pages": 0, "eng": 0, "home": 0, "flag": 0, "match": 0, "job": 0}
    data = gzip.GzipFile(fileobj=io.BytesIO(gz)).read()   # streaming: no quadratic copy across ~20k gzip members
    st["raw_mb"] = len(data) / 1e6
    out = []
    for hdr, body in records(data):
        if hdr.get(b"warc-type") != b"conversion":
            continue
        st["pages"] += 1
        if b"eng" not in hdr.get(b"warc-identified-content-language", b""):
            continue
        st["eng"] += 1
        url = hdr.get(b"warc-target-uri", b"").decode("utf-8", "replace")
        nl = body.find(b"\n")
        title = body[:nl if 0 <= nl < 400 else 0].decode("utf-8", "replace").strip()
        u = urlsplit(url)
        host = (u.hostname or "").lower()
        dom = reg_domain(host)
        # (a) names. Kept only when the domain relates to the name (gate), or for a distinctive multi-word name
        #     in the title of a top-level page (path depth <= 1). Body text is checked only for gated names.
        path = u.path or "/"
        label = dom.split(".")[0].replace("-", "").encode()
        cands = gate(label)
        shallow = path.strip("/").count("/") == 0
        ttoks = title.encode().replace(b"&", b" and ").translate(TABLE).split()
        seen = set()
        for i, pid in find(ttoks):
            if pid in seen:
                continue
            if pid in cands or (shallow and DISTINCT[pid] and len(META[pid][0].split()) > 1):
                seen.add(pid)
                out.append({"t": "m", "pid": pid, "where": "title", "url": url, "dom": dom, "title": title[:300],
                            "gated": pid in cands})
        if cands - seen:
            norm = b" " + b" ".join(body.replace(b"&", b" and ").translate(TABLE).split()) + b" "
            for pid in cands - seen:
                k = norm.find(NEEDLE[pid])
                if k >= 0:
                    seen.add(pid)
                    out.append({"t": "m", "pid": pid, "where": "body", "url": url, "dom": dom, "title": title[:300],
                                "gated": True, "snip": norm[max(0, k - 150): k + 200].decode("ascii", "replace")})
        st["match"] += len(seen)
        # (c) job pages in her role families (same RELEVANT / NOT_RELEVANT as board_scan)
        if (host.endswith(ATS_HOSTS) or JOB_PATH_RX.search(path)) and title and RELEVANT.search(title) \
                and not NOT_RELEVANT.search(title):
            st["job"] += 1
            out.append({"t": "j", "dom": dom, "host": host, "url": url, "title": title[:300], "family": family(title),
                        "ats": next((a for a in ATS_HOSTS if host.endswith(a)), "")})
        # (b) homepage scoring
        if u.query or not HOME_RX.match(path):
            continue
        st["home"] += 1
        low = body.lower()
        sig = [k for k, rx in LS_RX.items() if rx.search(low)]
        us = (2 if ZIP_RX.search(body) else 0) + (1 if PHONE_RX.search(body) else 0) + (1 if USA_RX.search(low) else 0)
        hosp = 1 if HOSP_RX.search(low) else 0
        areas = {a: sum(1 for rx in rxs if rx.search(low)) for a, rxs in AREA_RX.items()}
        flag_areas = []
        if len(sig) >= 3 or (len(sig) >= 2 and us >= 2):
            flag_areas.append("ls")
        if areas.get("pharma", 0) >= 2 or areas.get("medical devices", 0) >= 2:
            flag_areas.append("pharma/device")
        if areas.get("food", 0) >= 1 and FOOD_STRONG.search(low):
            flag_areas.append("food")
        if areas.get("cosmetics", 0) >= 1 and COSM_STRONG.search(low):
            flag_areas.append("cosmetics")
        if areas.get("hospitals", 0) >= 2:
            flag_areas.append("hospital")
        row = {"t": "h", "host": host, "dom": dom, "url": url, "title": title[:300], "ls": len(sig),
               "sig": ",".join(sig), "us": us, "hosp": hosp, "len": len(body), "flag": ",".join(flag_areas)}
        row.update({"a_" + a.split()[-1]: n for a, n in areas.items()})
        out.append(row)
        if flag_areas and us >= 1:
            st["flag"] += 1
            out.append({"t": "x", "dom": dom, "url": url, "text": body[:15000].decode("utf-8", "replace")})
    return out, st


# ------------------------------------------------------------------ WAT pass: careers / ATS links
ATS_LINK_RX = re.compile(rb'"url"\s*:\s*"(https?:(?:\\/|/)(?:\\/|/)[^"]*?(?:' +
                         b"|".join(re.escape(h.encode()) for h in ATS_HOSTS) + rb')[^"]*)"', re.I)
CAREER_LINK_RX = re.compile(rb'"url"\s*:\s*"([^"]{0,300}?(?:career|/jobs?\b|/jobs?/|join-?us|join-?our-?team|employment|'
                            rb'work-?with-?us|opportunities|vacanc)[^"]{0,200})"', re.I)
CAREER_TEXT_RX = re.compile(rb'"url"\s*:\s*"([^"]{1,400})"\s*,\s*"text"\s*:\s*"\s*(?:careers?|jobs|join (?:our|the) team|join us|'
                            rb'employment|work (?:with|for) us|current openings|open positions)\s*"', re.I)


URL_HOST_RX = re.compile(rb'"url"\s*:\s*"(https?:(?:\\?/){2}([^/"\\:?#]{3,120})[^"]{0,400})"')
ATS_HOSTS_B = tuple(h.encode() for h in ATS_HOSTS)
BAD_EXT = re.compile(r"\.(?:pdf|jpe?g|png|gif|svg|webp|docx?|xlsx?|zip|mp4|css|js)$", re.I)
CAREER_SUBS = {"careers", "career", "jobs", "talent", "join", "recruiting", "apply", "hiring", "workwithus"}
ATS_NEEDLES = tuple(h.split(".")[0].encode() for h in ATS_HOSTS)   # cheap C-level prefilter


# ---- homepage industry tags for the tech / automotive company list (companies + career links only; no postings)
TECH_RX = re.compile(r"\bsoftware\b|\bsaas\b|\bcloud\b|\bplatform\b|devops|kubernetes|\bdata (?:platform|analytics|"
                     r"infrastructure|engineering)\b|machine learning|artificial intelligence|\bai\b|\bmlops\b|"
                     r"cyber ?security|\bsecurity platform\b|\bdevelopers?\b|\bapis?\b|observability|\bdatabases?\b|"
                     r"semiconductor|\bcomputing\b|\bit services\b|\binfrastructure\b|\bautomation\b|\bfintech\b|"
                     r"\banalytics\b|\bllms?\b|\bgenerative ai\b|\bopen.source\b|\bsdk\b")
AUTO_RX = re.compile(r"\bautomotive\b|\bvehicles?\b|\belectric vehicles?\b|\bevs?\b|\badas\b|autonomous driving|"
                     r"self.driving|\bautomaker|\bcar ?maker|\btier.?1 supplier|\bpowertrain\b|\binfotainment\b|"
                     r"\bev charging\b|\blidar\b|\bautonomous vehicles?\b|\bmobility\b")
WAT_TITLE_RX = re.compile(rb'"Title"\s*:\s*"([^"]{3,300})"')
WAT_DESC_RX = re.compile(rb'"name"\s*:\s*"(?:og:)?description"\s*,\s*"content"\s*:\s*"([^"]{3,600})"', re.I)
WAT_DESC_RX2 = re.compile(rb'"content"\s*:\s*"([^"]{3,600})"\s*,\s*"name"\s*:\s*"(?:og:)?description"', re.I)


def board_key(url):
    """Normalize an ATS URL to its board: host + the path segment(s) that name the company."""
    u = urlsplit(url)
    host = (u.hostname or "").lower()
    seg = [s for s in u.path.split("/") if s]
    q = u.query
    if host.endswith(("myworkdayjobs.com", "myworkdaysite.com")):
        seg = [s for s in seg if not re.fullmatch(r"[a-z]{2}-[A-Z]{2}", s)]
        return host + "/" + (seg[0] if seg else "")
    if host in ("boards.greenhouse.io", "job-boards.greenhouse.io", "jobs.lever.co", "jobs.ashbyhq.com",
                "jobs.smartrecruiters.com", "careers.smartrecruiters.com", "apply.workable.com", "jobs.jobvite.com",
                "ats.rippling.com", "jobs.dayforcehcm.com"):
        if host.endswith("greenhouse.io") and seg[:1] == ["embed"]:
            m = re.search(r"for=([\w-]+)", q)
            return host + "/" + (m.group(1).lower() if m else "")
        return host + "/" + (seg[0].lower() if seg else "")
    if host == "recruiting.ultipro.com" or host.endswith("ukg.net"):
        return host + "/" + "/".join(seg[:1])
    if host == "recruiting.paylocity.com":
        return host + "/" + "/".join(seg[:4])
    if host == "workforcenow.adp.com":
        m = re.search(r"cid=([\w-]+)", q)
        return host + "/cid=" + (m.group(1) if m else "")
    if host.endswith("oraclecloud.com"):
        m = re.search(r"/sites/([\w-]+)", u.path)
        return host + "/" + (m.group(1) if m else "")
    if host.endswith(("successfactors.com", "successfactors.eu")):
        m = re.search(r"company=([\w-]+)", q, re.I)
        return host + "/" + (m.group(1) if m else "")
    return host   # subdomain-per-company systems: icims, bamboohr, recruitee, applytojob, taleo, breezy, jazzhr


def wat_records(fobj):
    """Stream (headers, content) from a WAT file without loading it all."""
    while True:
        line = fobj.readline()
        if not line:
            return
        if not line.startswith(b"WARC/"):
            continue
        hdr = {}
        while True:
            line = fobj.readline()
            if not line or line in (b"\r\n", b"\n"):
                break
            k, _, v = line.partition(b":")
            hdr[k.strip().lower()] = v.strip()
        yield hdr, fobj.read(int(hdr.get(b"content-length", 0)))


def scan_wat(gz):
    from urllib.parse import urljoin
    st = {"gz_mb": len(gz) / 1e6, "records": 0, "ats": 0, "career": 0}
    out, seen = [], set()
    for hdr, body in wat_records(gzip.GzipFile(fileobj=io.BytesIO(gz))):
        if hdr.get(b"warc-type") != b"metadata" or b'"Links"' not in body:
            continue
        try:
            wat_one(hdr, body, st, out, seen)
        except ValueError:                      # malformed URLs (urlsplit/hostname)
            st["bad"] = st.get("bad", 0) + 1
    return out, st


def wat_one(hdr, body, st, out, seen):
    from urllib.parse import urljoin
    if True:
        uri = hdr.get(b"warc-target-uri", b"").decode("utf-8", "replace")
        if not uri.startswith("http"):
            return
        st["records"] += 1
        u = urlsplit(uri)
        host = (u.hostname or "").lower()
        dom = reg_domain(host)
        for m in URL_HOST_RX.finditer(body):           # one pass: host of every absolute link
            if not m.group(2).lower().endswith(ATS_HOSTS_B):
                continue
            link = m.group(1).replace(b"\\/", b"/").decode("utf-8", "replace")
            try:
                key = board_key(link)
            except ValueError:
                continue
            if (dom, key) in seen:
                continue
            seen.add((dom, key))
            st["ats"] += 1
            out.append({"t": "a", "dom": dom, "page": uri[:500], "board": key, "link": link[:500],
                        "self": host.endswith(ATS_HOSTS)})
        sub = host.split(".")[0]
        if sub in CAREER_SUBS and not host.endswith(ATS_HOSTS) and (dom, host) not in seen:
            seen.add((dom, host))                # careers.acme.com / jobs.acme.com = a career site
            st["career"] += 1
            out.append({"t": "c", "dom": dom, "page": uri[:500], "link": f"{u.scheme}://{host}/", "sub": True})
        # careers link from ANY page of the site (footer/nav), once per domain per file
        if dom not in seen and not host.endswith(ATS_HOSTS) and \
                (b"areer" in body or b"/jobs" in body or b"Jobs" in body or b"Join" in body or b"mploy" in body):
            links = [m.group(1) for m in CAREER_TEXT_RX.finditer(body)] + \
                    [m.group(1) for m in CAREER_LINK_RX.finditer(body)]
            for raw in links[:20]:
                link = urljoin(uri, raw.replace(b"\\/", b"/").decode("utf-8", "replace"))
                p = urlsplit(link)
                lh = (p.hostname or "").lower()
                if not lh or (reg_domain(lh) != dom and not lh.endswith(ATS_HOSTS)) or BAD_EXT.search(p.path) \
                        or p.path.count("/") > 4:
                    continue
                seen.add(dom)
                st["career"] += 1
                out.append({"t": "c", "dom": dom, "page": uri[:500], "link": link[:500]})
                break
        if u.query or not HOME_RX.match(u.path or "/") or host.endswith(ATS_HOSTS):
            return
        tm = WAT_TITLE_RX.search(body)
        dm = WAT_DESC_RX.search(body) or WAT_DESC_RX2.search(body)
        title = tm.group(1).decode("utf-8", "replace") if tm else ""
        desc = dm.group(1).decode("utf-8", "replace") if dm else ""
        text = (title + " " + desc).lower()
        tech = sorted({m.group(0) for m in TECH_RX.finditer(text)})
        auto = sorted({m.group(0) for m in AUTO_RX.finditer(text)})
        if len(tech) >= 2 or auto:
            st["industry"] = st.get("industry", 0) + 1
            out.append({"t": "i", "dom": dom, "url": uri[:300], "title": title[:200], "desc": desc[:300],
                        "tech": ",".join(tech), "auto": ",".join(auto)})


def out_name(src):
    return os.path.basename(src).replace(".warc.wet.gz", "") + ".jsonl.gz"


_S3 = None


def s3():
    global _S3
    if _S3 is None:
        import boto3
        from botocore.config import Config
        _S3 = boto3.client("s3", config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))
    return _S3


def work(args):
    src, outdir, deadline = args
    if deadline and time.time() > deadline:
        return {"src": src, "skipped": 1}
    name = out_name(src)
    t0 = time.time()
    try:
        gz = fetch(src)
    except Exception as e:  # noqa: BLE001
        return {"src": src, "error": f"fetch: {e}"[:300]}
    t1, c1 = time.time(), time.process_time()
    try:
        out, st = scan_wat(gz) if ".wat." in src else scan_bytes(gz)
    except Exception as e:  # noqa: BLE001
        return {"src": src, "error": f"scan: {e}"[:300]}
    st.update(src=src, fetch_s=round(t1 - t0, 2), cpu_s=round(time.process_time() - c1, 2))
    out.append({"t": "s", **st})
    blob = gzip.compress("\n".join(json.dumps(r, ensure_ascii=False) for r in out).encode(), 5)
    if outdir.startswith("s3://"):
        b, _, pre = outdir[5:].partition("/")
        s3().put_object(Bucket=b, Key=f"{pre.rstrip('/')}/{name}", Body=blob)
    else:
        with open(os.path.join(outdir, name), "wb") as f:
            f.write(blob)
    return st


def existing(outdir):
    if outdir.startswith("s3://"):
        b, _, pre = outdir[5:].partition("/")
        names = set()
        for page in s3().get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=pre.rstrip("/") + "/"):
            names.update(os.path.basename(o["Key"]) for o in page.get("Contents", []))
        return names
    os.makedirs(outdir, exist_ok=True)
    return set(os.listdir(outdir))


def init(path, rules):
    load_patterns(path)
    load_rules(rules)


def run(pool, todo, tot, counts):
    t0 = time.time()
    for st in pool.imap_unordered(work, todo, chunksize=1):
        if "skipped" in st:
            counts["skipped"] += 1
            continue
        if "error" in st:
            counts["errors"] += 1
            print("ERR", st, flush=True)
            continue
        counts["files"] += 1
        n = counts["files"]
        for k, v in st.items():
            if isinstance(v, (int, float)):
                tot[k] = tot.get(k, 0) + v
        if n <= 10 or n % 25 == 0:
            el = time.time() - t0
            print(f"{n} files  {el:.0f}s  {tot['gz_mb'] / max(el, 1):.1f} MB/s gz  cpu/file {tot['cpu_s'] / n:.1f}s  "
                  f"fetch/file {tot['fetch_s'] / n:.1f}s  errs {counts['errors']}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--patterns", required=True)
    ap.add_argument("--list", required=True)
    ap.add_argument("--all", help="full source list; after --list, sweep files nobody finished (random order)")
    ap.add_argument("--rules", default="rules.json")
    ap.add_argument("--out", required=True)
    ap.add_argument("--procs", type=int, default=os.cpu_count())
    ap.add_argument("--minutes", type=float, default=0, help="stop starting new files after this many minutes")
    a = ap.parse_args()
    import random
    deadline = time.time() + a.minutes * 60 if a.minutes else 0
    t0, tot, counts = time.time(), {}, {"files": 0, "errors": 0, "skipped": 0}
    with Pool(a.procs, initializer=init, initargs=(a.patterns, a.rules)) as pool:
        srcs = [l.strip() for l in open(a.list) if l.strip()]
        done = existing(a.out)
        todo = [s for s in srcs if out_name(s) not in done]
        print(f"{len(srcs)} sources, {len(srcs) - len(todo)} already done, {len(todo)} to scan, {a.procs} procs", flush=True)
        run(pool, [(s, a.out, deadline) for s in todo], tot, counts)
        for sweep in range(3 if a.all else 0):
            if deadline and time.time() > deadline:
                break
            done = existing(a.out)
            rest = [s.strip() for s in open(a.all) if s.strip() and out_name(s.strip()) not in done]
            print(f"sweep {sweep + 1}: {len(rest)} files left overall", flush=True)
            if not rest:
                break
            random.shuffle(rest)
            run(pool, [(s, a.out, deadline) for s in rest], tot, counts)
    summary = {k: round(v, 1) for k, v in tot.items()}
    summary.update(counts, wall_s=round(time.time() - t0, 1))
    print("SUMMARY", json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
