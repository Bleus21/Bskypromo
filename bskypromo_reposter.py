from atproto import Client
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Set, Tuple

# ================== ENV ==================

BSKY_USERNAME = os.getenv("BSKY_USERNAME")
BSKY_PASSWORD = os.getenv("BSKY_PASSWORD")

FEED_LINK = os.getenv(
    "FEED_LINK",
    "https://bsky.app/profile/did:plc:jaka644beit3x4vmmg6yysw7/feed/aaaipcjvdtvu4",
)

EXCLUDE_LIST_LINK = os.getenv(
    "EXCLUDE_LIST_LINK",
    "https://bsky.app/profile/did:plc:5si6ivvplllayxrf6h5euwsd/lists/3mfkghzcmt72w",
)

MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "100"))
POST_DELAY_SECONDS = float(os.getenv("POST_DELAY_SECONDS", "1.2"))

# Set HOURS_BACK=0 to ignore age completely (recommended if feed "reshuffles" old posts)
HOURS_BACK = int(os.getenv("HOURS_BACK", "0"))

# Keeps reposted.txt small (removes old log lines). Does NOT delete old reposts from your profile.
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "14"))

FEED_MAX_ITEMS = int(os.getenv("FEED_MAX_ITEMS", "1000"))
LIST_MEMBER_LIMIT = int(os.getenv("LIST_MEMBER_LIMIT", "500"))

REPOST_LOG_FILE = os.getenv("REPOST_LOG_FILE", "reposted.txt")

# ================== REGEX ==================

LIST_URL_RE = re.compile(r"https://bsky\.app/profile/([^/]+)/lists/([^/?#]+)", re.I)
FEED_URL_RE = re.compile(r"https://bsky\.app/profile/([^/]+)/feed/([^/?#]+)", re.I)

# ================== TIME ==================

def now_dt() -> datetime:
    return datetime.now(timezone.utc)

def now_iso() -> str:
    return now_dt().strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_dt(val: str) -> Optional[datetime]:
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return None

def robust_post_time(post) -> datetime:
    record = getattr(post, "record", None)
    created = getattr(record, "createdAt", None) if record else None
    dt = parse_dt(created) if created else None
    if dt:
        return dt
    indexed = getattr(post, "indexedAt", None)
    dt2 = parse_dt(indexed) if indexed else None
    if dt2:
        return dt2
    return datetime(1970, 1, 1, tzinfo=timezone.utc)

# ================== reposted.txt ==================

def load_reposted_log(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    out = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # format: "ISO_TIMESTAMP\tPOST_URI"
            parts = line.split("\t", 1)
            if len(parts) == 2:
                out.add(parts[1].strip())
            else:
                out.add(parts[0].strip())
    return out

def cleanup_reposted_log(path: str, days: int) -> None:
    if not os.path.exists(path):
        return
    cutoff = now_dt() - timedelta(days=days)
    kept_lines = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t", 1)
            if len(parts) != 2:
                # legacy line without timestamp: keep it
                kept_lines.append(line)
                continue
            ts, uri = parts[0].strip(), parts[1].strip()
            dt = parse_dt(ts)
            if not dt or dt >= cutoff:
                kept_lines.append(f"{ts}\t{uri}")

    with open(path, "w", encoding="utf-8") as f:
        for l in kept_lines:
            f.write(l + "\n")

def append_reposted(path: str, uri: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{now_iso()}\t{uri}\n")

# ================== URI NORMALIZERS ==================

def resolve_handle_to_did(client: Client, actor: str) -> Optional[str]:
    if actor.startswith("did:"):
        return actor
    try:
        out = client.com.atproto.identity.resolve_handle({"handle": actor})
        return getattr(out, "did", None)
    except Exception:
        return None

def normalize_list_uri(client: Client, link: str) -> Optional[str]:
    if not link:
        return None
    if link.startswith("at://"):
        return link
    m = LIST_URL_RE.match(link)
    if not m:
        return None
    did = resolve_handle_to_did(client, m.group(1))
    if not did:
        return None
    return f"at://{did}/app.bsky.graph.list/{m.group(2)}"

def normalize_feed_uri(client: Client, link: str) -> Optional[str]:
    if not link:
        return None
    if link.startswith("at://"):
        return link
    m = FEED_URL_RE.match(link)
    if not m:
        return None
    did = resolve_handle_to_did(client, m.group(1))
    if not did:
        return None
    return f"at://{did}/app.bsky.feed.generator/{m.group(2)}"

# ================== FETCHERS ==================

def fetch_list_members(client: Client, list_uri: str, limit: int) -> List[str]:
    members, cursor = [], None
    while True:
        params = {"list": list_uri, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        out = client.app.bsky.graph.get_list(params)
        for it in getattr(out, "items", []) or []:
            subj = getattr(it, "subject", None)
            did = getattr(subj, "did", None) if subj else None
            if did:
                members.append(did)
            if len(members) >= limit:
                return members[:limit]
        cursor = getattr(out, "cursor", None)
        if not cursor:
            break
    return members[:limit]

def fetch_feed_items(client: Client, feed_uri: str, max_items: int) -> List:
    items, cursor = [], None
    while len(items) < max_items:
        batch_limit = min(100, max_items - len(items))
        if batch_limit <= 0:
            break
        params = {"feed": feed_uri, "limit": batch_limit}
        if cursor:
            params["cursor"] = cursor
        out = client.app.bsky.feed.get_feed(params)
        batch = getattr(out, "feed", []) or []
        items.extend(batch)
        cursor = getattr(out, "cursor", None)
        if not cursor or not batch:
            break
    return items[:max_items]

# ================== MEDIA CHECK ==================

def has_media(record) -> bool:
    embed = getattr(record, "embed", None)
    if not embed:
        return False
    if getattr(embed, "images", None):
        return True
    if getattr(embed, "video", None):
        return True
    rwm = getattr(embed, "recordWithMedia", None)
    if rwm:
        media = getattr(rwm, "media", None)
        if media:
            if getattr(media, "images", None):
                return True
            if getattr(media, "video", None):
                return True
    return False

# ================== MAIN ==================

def main():
    if not BSKY_USERNAME or not BSKY_PASSWORD:
        print("❌ Missing BSKY_USERNAME / BSKY_PASSWORD")
        return

    client = Client()
    client.login(BSKY_USERNAME, BSKY_PASSWORD)
    print(f"✅ Logged in as {client.me.did}")

    # cleanup reposted.txt (keeps file small)
    cleanup_reposted_log(REPOST_LOG_FILE, CLEANUP_DAYS)

    done = load_reposted_log(REPOST_LOG_FILE)
    print(f"🧾 Loaded {len(done)} reposted URIs from {REPOST_LOG_FILE}")

    # Exclude list
    exclude_dids: Set[str] = set()
    if EXCLUDE_LIST_LINK:
        ex_uri = normalize_list_uri(client, EXCLUDE_LIST_LINK)
        if not ex_uri:
            print("❌ EXCLUDE_LIST_LINK invalid")
            return
        exclude_dids = set(fetch_list_members(client, ex_uri, LIST_MEMBER_LIMIT))
        print(f"🚫 Loaded {len(exclude_dids)} excluded authors")

    # Feed
    feed_uri = normalize_feed_uri(client, FEED_LINK)
    if not feed_uri:
        print("❌ FEED_LINK invalid")
        return

    cutoff = now_dt() - timedelta(hours=HOURS_BACK) if HOURS_BACK > 0 else None

    feed_items = fetch_feed_items(client, feed_uri, FEED_MAX_ITEMS)
    print(f"🧲 Feed items fetched: {len(feed_items)}")

    candidates: List[Tuple[datetime, str, str]] = []
    for it in feed_items:
        post = getattr(it, "post", None)
        if not post:
            continue

        uri = getattr(post, "uri", None)
        cid = getattr(post, "cid", None)
        author = getattr(getattr(post, "author", None), "did", None)
        record = getattr(post, "record", None)

        if not uri or not cid or not author or not record:
            continue

        if author in exclude_dids:
            continue

        if uri in done:
            continue

        p_time = robust_post_time(post)
        if cutoff and p_time < cutoff:
            continue

        if not has_media(record):
            continue

        candidates.append((p_time, uri, cid))

    # Oldest -> newest (so newest ends up on top after the run)
    candidates.sort(key=lambda x: x[0])

    made = 0
    for p_time, uri, cid in candidates:
        if made >= MAX_PER_RUN:
            break
        try:
            ts = now_iso()

            client.app.bsky.feed.repost.create(
                repo=client.me.did,
                record={"subject": {"uri": uri, "cid": cid}, "createdAt": ts},
            )
            client.app.bsky.feed.like.create(
                repo=client.me.did,
                record={"subject": {"uri": uri, "cid": cid}, "createdAt": ts},
            )

            append_reposted(REPOST_LOG_FILE, uri)
            done.add(uri)
            made += 1

            print(f"✅ Reposted+liked: {uri} | post_time={p_time.isoformat()}")
            time.sleep(POST_DELAY_SECONDS)

        except Exception as e:
            print(f"⚠️ Error for {uri}: {e}")
            time.sleep(2)

    print(f"🔥 Done — reposted: {made}")

if __name__ == "__main__":
    main()