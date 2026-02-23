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

# ✅ Source feed (your custom feed)
FEED_LINK = os.getenv(
    "FEED_LINK",
    "https://bsky.app/profile/did:plc:jaka644beit3x4vmmg6yysw7/feed/aaaipcjvdtvu4",
)

HASHTAG = os.getenv("HASHTAG", "#bskypromo")

MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "100"))
POST_DELAY_SECONDS = float(os.getenv("POST_DELAY_SECONDS", "1.2"))

HOURS_BACK = int(os.getenv("HOURS_BACK", "24"))
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "14"))

FOLLOW_LIST_LINK = os.getenv("FOLLOW_LIST_LINK", "")
LIST_MEMBER_LIMIT = int(os.getenv("LIST_MEMBER_LIMIT", "500"))

# feed paging
FEED_MAX_ITEMS = int(os.getenv("FEED_MAX_ITEMS", "1000"))  # max items we pull per run
STATE_FILE = os.getenv("STATE_FILE", "state.json")

# ================== REGEX ==================

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
        return {"reposts": [], "followed": []}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

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

def extract_post_text(post) -> str:
    record = getattr(post, "record", None)
    return getattr(record, "text", "") if record else ""

def contains_hashtag(text: str, tag: str) -> bool:
    # case-insensitive, “losse hashtag” match
    return re.search(rf"(?i)(^|\s){re.escape(tag)}(\s|$|[!,.?:;])", text or "") is not None

def has_media(record) -> bool:
    embed = getattr(record, "embed", None)
    if not embed:
        return False

    imgs = getattr(embed, "images", None)
    if imgs and len(imgs) > 0:
        return True

    if getattr(embed, "video", None):
        return True

    rwm = getattr(embed, "recordWithMedia", None)
    if rwm:
        media = getattr(rwm, "media", None)
        if media:
            imgs2 = getattr(media, "images", None)
            if imgs2 and len(imgs2) > 0:
                return True
            if getattr(media, "video", None):
                return True

    return False

def robust_post_time_from_post(post) -> datetime:
    """
    Stable timestamp for sorting:
    1) record.createdAt
    2) post.indexedAt (if present)
    3) epoch (never now -> ordering won't flip)
    """
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

# ================== FOLLOW LIST MEMBERS ==================

def follow_list_members(client: Client, list_members: Set[str], state: Dict[str, Any]) -> None:
    """
    Follow members from your manually-managed list (only follow; no unfollow).
    """
    already_followed: Set[str] = set(state.get("followed", []))

    for did in list_members:
        if did == client.me.did:
            continue
        if did in already_followed:
            continue

        try:
            prof = client.app.bsky.actor.get_profile({"actor": did})
            is_following = bool(getattr(getattr(prof, "viewer", None), "following", None))
            if not is_following:
                client.app.bsky.graph.follow.create(
                    repo=client.me.did,
                    record={"subject": did, "createdAt": now_iso()},
                )
                print(f"✅ Followed (list): {did}")

            already_followed.add(did)
            time.sleep(0.15)
        except Exception as e:
            print(f"⚠️ Follow error {did}: {e}")

    state["followed"] = sorted(already_followed)

# ================== FEED FETCH (PAGED) ==================

def fetch_feed_items(client: Client, feed_uri: str, max_items: int) -> List:
    """
    app.bsky.feed.get_feed limit <= 100; page via cursor until max_items.
    """
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
    """
    repost_uri: at://did/app.bsky.feed.repost/rkey
    """
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
    except Exception as e:
        print(f"⚠️ Delete repost error: {e}")
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
            print(f"🧹 Deleted old repost: {repost_uri}")
        else:
            keep.append(item)

        time.sleep(0.15)

    state["reposts"] = keep
    return removed

# ================== REPOST + LIKE (FROM FEED) ==================

def build_candidates_from_feed(feed_items: List, allowed_authors: Set[str], cutoff: datetime) -> List[Tuple[datetime, str, str, str]]:
    """
    Returns list of tuples:
    (post_time, post_uri, post_cid, author_did)
    """
    candidates: List[Tuple[datetime, str, str, str]] = []

    for it in feed_items:
        post = getattr(it, "post", None)
        if not post:
            continue

        post_uri = getattr(post, "uri", None)
        post_cid = getattr(post, "cid", None)
        author_did = getattr(getattr(post, "author", None), "did", None)
        record = getattr(post, "record", None)

        if not post_uri or not post_cid or not author_did or not record:
            continue

        # allowlist
        if author_did not in allowed_authors:
            continue

        # time window
        p_time = robust_post_time_from_post(post)
        if p_time < cutoff:
            continue

        # hashtag + media (replies allowed)
        txt = extract_post_text(post)
        if not contains_hashtag(txt, HASHTAG):
            continue
        if not has_media(record):
            continue

        candidates.append((p_time, post_uri, post_cid, author_did))

    return candidates

def do_reposts_and_likes_from_feed(
    client: Client,
    state: Dict[str, Any],
    allowed_authors: Set[str],
    feed_uri: str,
) -> int:
    cutoff = now_dt() - timedelta(hours=HOURS_BACK)
    known_post_uris = {x.get("post_uri") for x in state.get("reposts", []) if x.get("post_uri")}

    feed_items = fetch_feed_items(client, feed_uri, FEED_MAX_ITEMS)
    candidates = build_candidates_from_feed(feed_items, allowed_authors, cutoff)

    # ✅ Oldest-first so newest ends up on top (last repost is newest original)
    candidates.sort(key=lambda x: x[0])

    made = 0
    for p_time, post_uri, post_cid, author_did in candidates:
        if made >= MAX_PER_RUN:
            break
        if post_uri in known_post_uris:
            continue

        try:
            ts = now_iso()

            created_out = client.app.bsky.feed.repost.create(
                repo=client.me.did,
                record={"subject": {"uri": post_uri, "cid": post_cid}, "createdAt": ts},
            )
            repost_uri = getattr(created_out, "uri", None)

            client.app.bsky.feed.like.create(
                repo=client.me.did,
                record={"subject": {"uri": post_uri, "cid": post_cid}, "createdAt": ts},
            )

            state.setdefault("reposts", []).append({
                "post_uri": post_uri,
                "post_cid": post_cid,
                "repost_uri": repost_uri,
                "createdAt": ts
            })

            known_post_uris.add(post_uri)
            made += 1

            print(f"✅ Reposted+liked: {post_uri} | post_time={p_time.isoformat()} | author={author_did}")
            time.sleep(POST_DELAY_SECONDS)

        except Exception as e:
            print(f"⚠️ Repost/like error {post_uri}: {e}")
            time.sleep(2.0)

    return made

# ================== MAIN ==================

def main():
    if not BSKY_USERNAME or not BSKY_PASSWORD:
        print("❌ Missing BSKY_USERNAME / BSKY_PASSWORD")
        return

    client = Client()
    client.login(BSKY_USERNAME, BSKY_PASSWORD)
    print(f"✅ Logged in as {client.me.did}")

    state = load_state(STATE_FILE)

    # list -> allowlist
    list_uri = normalize_list_uri(client, FOLLOW_LIST_LINK)
    if not list_uri:
        print("❌ FOLLOW_LIST_LINK invalid / could not normalize.")
        return

    allowed_authors = set(fetch_list_members(client, list_uri, LIST_MEMBER_LIMIT))
    print(f"📋 Loaded {len(allowed_authors)} authors from list")

    # follow (bijvolgen)
    follow_list_members(client, allowed_authors, state)

    # cleanup
    removed = cleanup_old_reposts(client, state)

    # feed -> repost + like
    feed_uri = normalize_feed_uri(client, FEED_LINK)
    if not feed_uri:
        print("❌ FEED_LINK invalid / could not normalize.")
        return

    print(f"🧲 Using feed: {feed_uri}")
    made = do_reposts_and_likes_from_feed(client, state, allowed_authors, feed_uri)

    save_state(STATE_FILE, state)
    print(f"🔥 Done — reposted+liked: {made}, cleaned: {removed}, tracked: {len(state.get('reposts', []))}")

if __name__ == "__main__":
    main()