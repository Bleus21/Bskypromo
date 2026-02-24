from atproto import Client
import os
import re
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Set, Tuple

# ================== ENV CONFIG ==================

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

HOURS_BACK = int(os.getenv("HOURS_BACK", "24"))
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "14"))

FEED_MAX_ITEMS = int(os.getenv("FEED_MAX_ITEMS", "1000"))
LIST_MEMBER_LIMIT = int(os.getenv("LIST_MEMBER_LIMIT", "500"))

STATE_FILE = os.getenv("STATE_FILE", "state.json")

# Anti-dup safety
DUP_CHECK_AUTHOR_FEED_LIMIT = int(os.getenv("DUP_CHECK_AUTHOR_FEED_LIMIT", "100"))  # <= 100
DUP_CHECK_CACHE = int(os.getenv("DUP_CHECK_CACHE", "4000"))

# Debug + switches
DEBUG = os.getenv("DEBUG", "0") == "1"
MEDIA_ONLY = os.getenv("MEDIA_ONLY", "1") == "1"  # set to 0 to test quickly

LIST_URL_RE = re.compile(r"https://bsky\.app/profile/([^/]+)/lists/([^/?#]+)", re.I)
FEED_URL_RE = re.compile(r"https://bsky\.app/profile/([^/]+)/feed/([^/?#]+)", re.I)

# ================== UTILS ==================

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

def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"reposts": [], "seen_reposted": [], "seen_liked": []}
    with open(path, "r", encoding="utf-8") as f:
        st = json.load(f)
    st.setdefault("reposts", [])
    st.setdefault("seen_reposted", [])
    st.setdefault("seen_liked", [])
    return st

def save_state(path: str, state: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

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

# ================== FEED FETCH (PAGED) ==================

def fetch_feed_items(client: Client, feed_uri: str, max_items: int) -> List:
    items: List = []
    cursor = None
    max_items = int(max_items)

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

# ================== CLEANUP ==================

def delete_repost_record(client: Client, repost_uri: str) -> bool:
    try:
        if not repost_uri or not repost_uri.startswith("at://"):
            return False
        parts = repost_uri.split("/")
        if len(parts) < 5:
            return False
        did = parts[2]
        collection = parts[3]
        rkey = parts[4]
        if did != client.me.did:
            return False
        client.com.atproto.repo.delete_record({
            "repo": client.me.did,
            "collection": collection,
            "rkey": rkey
        })
        return True
    except Exception:
        return False

def cleanup_old_reposts(client: Client, state: Dict[str, Any]) -> int:
    cutoff = now_dt() - timedelta(days=CLEANUP_DAYS)
    keep = []
    removed = 0
    for item in state.get("reposts", []):
        created_dt = parse_dt(item.get("createdAt", "")) or now_dt()
        if created_dt >= cutoff:
            keep.append(item)
            continue
        repost_uri = item.get("repost_uri")
        if repost_uri and delete_repost_record(client, repost_uri):
            removed += 1
        else:
            keep.append(item)
    state["reposts"] = keep
    return removed

# ================== ANTI-DUP SAFETY ==================

def build_viewer_sets_from_own_feed(client: Client, limit: int) -> Tuple[Set[str], Set[str]]:
    reposted_uris: Set[str] = set()
    liked_uris: Set[str] = set()
    try:
        out = client.app.bsky.feed.get_author_feed({"actor": client.me.did, "limit": min(int(limit), 100)})
        feed = getattr(out, "feed", []) or []
    except Exception:
        return reposted_uris, liked_uris

    for it in feed:
        post = getattr(it, "post", None)
        if not post:
            continue
        uri = getattr(post, "uri", None)
        viewer = getattr(post, "viewer", None)
        if not uri or not viewer:
            continue
        if getattr(viewer, "repost", None):
            reposted_uris.add(uri)
        if getattr(viewer, "like", None):
            liked_uris.add(uri)
    return reposted_uris, liked_uris

def update_seen_cache(state: Dict[str, Any], reposted: Set[str], liked: Set[str]) -> None:
    sr = list(dict.fromkeys(list(reposted) + state.get("seen_reposted", [])))[:DUP_CHECK_CACHE]
    sl = list(dict.fromkeys(list(liked) + state.get("seen_liked", [])))[:DUP_CHECK_CACHE]
    state["seen_reposted"] = sr
    state["seen_liked"] = sl

# ================== REPOST + LIKE ==================

def do_reposts(client: Client, state: Dict[str, Any], feed_uri: str, exclude_authors: Set[str]) -> int:
    cutoff = now_dt() - timedelta(hours=HOURS_BACK)

    known_state = {x.get("post_uri") for x in state.get("reposts", []) if x.get("post_uri")}
    api_reposted, api_liked = build_viewer_sets_from_own_feed(client, DUP_CHECK_AUTHOR_FEED_LIMIT)
    cached_reposted = set(state.get("seen_reposted", []))
    cached_liked = set(state.get("seen_liked", []))

    already_reposted = api_reposted | cached_reposted | known_state
    already_liked = api_liked | cached_liked

    feed_items = fetch_feed_items(client, feed_uri, FEED_MAX_ITEMS)
    print(f"🧪 feed_items fetched: {len(feed_items)} | cutoff={cutoff.isoformat()} | media_only={MEDIA_ONLY}")

    counts = {"missing": 0, "excluded": 0, "already": 0, "too_old": 0, "no_media": 0, "ok": 0}
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
            counts["missing"] += 1
            continue
        if author in exclude_authors:
            counts["excluded"] += 1
            continue
        if uri in already_reposted:
            counts["already"] += 1
            continue

        p_time = robust_post_time(post)
        if p_time < cutoff:
            counts["too_old"] += 1
            continue

        if MEDIA_ONLY and not has_media(record):
            counts["no_media"] += 1
            continue

        counts["ok"] += 1
        candidates.append((p_time, uri, cid))

    print("🧪 counts:", counts)

    # Oldest-first so newest original ends up top
    candidates.sort(key=lambda x: x[0])

    made = 0
    for p_time, uri, cid in candidates:
        if made >= MAX_PER_RUN:
            break
        try:
            ts = now_iso()

            out = client.app.bsky.feed.repost.create(
                repo=client.me.did,
                record={"subject": {"uri": uri, "cid": cid}, "createdAt": ts},
            )
            repost_uri = getattr(out, "uri", None)

            if uri not in already_liked:
                client.app.bsky.feed.like.create(
                    repo=client.me.did,
                    record={"subject": {"uri": uri, "cid": cid}, "createdAt": ts},
                )
                already_liked.add(uri)

            state["reposts"].append({
                "post_uri": uri,
                "post_cid": cid,
                "repost_uri": repost_uri,
                "createdAt": ts
            })
            already_reposted.add(uri)

            made += 1
            print(f"✅ Reposted+liked: {uri} | post_time={p_time.isoformat()}")
            time.sleep(POST_DELAY_SECONDS)

        except Exception as e:
            print(f"⚠️ Error for {uri}: {e}")
            time.sleep(2)

    update_seen_cache(state, already_reposted, already_liked)
    return made

# ================== MAIN ==================

def main():
    if not BSKY_USERNAME or not BSKY_PASSWORD:
        print("❌ Missing credentials: BSKY_USERNAME / BSKY_PASSWORD")
        return

    client = Client()
    client.login(BSKY_USERNAME, BSKY_PASSWORD)
    print(f"✅ Logged in as {client.me.did}")

    state = load_state(STATE_FILE)

    exclude_authors: Set[str] = set()
    if EXCLUDE_LIST_LINK:
        ex_uri = normalize_list_uri(client, EXCLUDE_LIST_LINK)
        if not ex_uri:
            print("❌ EXCLUDE_LIST_LINK invalid / could not normalize.")
            return
        exclude_authors = set(fetch_list_members(client, ex_uri, LIST_MEMBER_LIMIT))
        print(f"🚫 Loaded {len(exclude_authors)} excluded authors")
    else:
        print("ℹ️ No EXCLUDE_LIST_LINK set; exclude list is empty")

    removed = cleanup_old_reposts(client, state)

    feed_uri = normalize_feed_uri(client, FEED_LINK)
    if not feed_uri:
        print("❌ FEED_LINK invalid / could not normalize.")
        return

    print(f"🧲 Using feed: {feed_uri}")
    made = do_reposts(client, state, feed_uri, exclude_authors)

    save_state(STATE_FILE, state)
    print(f"🔥 Done — reposted: {made}, cleaned: {removed}, tracked: {len(state.get('reposts', []))}")

if __name__ == "__main__":
    main()