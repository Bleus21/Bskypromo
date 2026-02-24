from atproto import Client
import os
import re
import time
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Set, Tuple

# Github Actions: print direct
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

print("=== BSKYPROMO HASHTAG BOT STARTED ===", flush=True)

# ============================================================
# CONFIG — leeg = skip (structuur blijft bestaan)
# ============================================================

FEEDS = {
    "feed 1": {"link": "", "note": ""},
    "feed 2": {"link": "", "note": ""},
    "feed 3": {"link": "", "note": ""},
    "feed 4": {"link": "", "note": ""},
    "feed 5": {"link": "", "note": ""},
}

LIJSTEN = {
    "lijst 1": {"link": "", "note": ""},
    "lijst 2": {"link": "", "note": ""},
    "lijst 3": {"link": "", "note": ""},
    "lijst 4": {"link": "", "note": ""},
    "lijst 5": {"link": "", "note": ""},
}

EXCLUDE_LISTS = {
    # leeg = geen exclude actief
}

HASHTAG_QUERY = "#bskypromo"

PROMO_FEED_KEY = "feed 1"
PROMO_LIST_KEY = "lijst 1"

# ============================================================
# RUNTIME CONFIG (env)
# ============================================================
HOURS_BACK = int(os.getenv("HOURS_BACK", "3"))
MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "50"))
MAX_PER_USER = int(os.getenv("MAX_PER_USER", "3"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "2"))

STATE_FILE = os.getenv("STATE_FILE", "state.json")

FEED_MAX_ITEMS = int(os.getenv("FEED_MAX_ITEMS", "500"))
HASHTAG_MAX_ITEMS = int(os.getenv("HASHTAG_MAX_ITEMS", "100"))

# Secrets in GitHub
ENV_USERNAME = "BSKY_USERNAME"
ENV_PASSWORD = "BSKY_PASSWORD"

# ============================================================
# REGEX (blijft staan voor compat/template; nu niet gebruikt)
# ============================================================
FEED_URL_RE = re.compile(r"^https?://(www\.)?bsky\.app/profile/([^/]+)/feed/([^/?#]+)", re.I)
LIST_URL_RE = re.compile(r"^https?://(www\.)?bsky\.app/profile/([^/]+)/lists/([^/?#]+)", re.I)


def log(msg: str):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(post) -> Optional[datetime]:
    indexed = getattr(post, "indexedAt", None) or getattr(post, "indexed_at", None)
    if indexed:
        try:
            return datetime.fromisoformat(indexed.replace("Z", "+00:00"))
        except Exception:
            pass

    record = getattr(post, "record", None)
    if record:
        created = getattr(record, "createdAt", None) or getattr(record, "created_at", None)
        if created:
            try:
                return datetime.fromisoformat(created.replace("Z", "+00:00"))
            except Exception:
                pass
    return None


def is_quote_post(record) -> bool:
    embed = getattr(record, "embed", None)
    if not embed:
        return False
    return bool(getattr(embed, "record", None) or getattr(embed, "recordWithMedia", None))


def has_media(record) -> bool:
    """
    Alleen echte media: images/video.
    External-only (link-card) telt NIET als media.
    """
    embed = getattr(record, "embed", None)
    if not embed:
        return False

    if getattr(embed, "images", None):
        return True
    if getattr(embed, "video", None):
        return True

    if getattr(embed, "external", None):
        return False

    rwm = getattr(embed, "recordWithMedia", None)
    if rwm and getattr(rwm, "media", None):
        m = rwm.media
        if getattr(m, "images", None):
            return True
        if getattr(m, "video", None):
            return True

    return False


def load_state(path: str) -> Dict:
    if not os.path.exists(path):
        return {"repost_records": {}, "like_records": {}}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(path: str, state: Dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def fetch_hashtag_posts(client: Client, max_items: int) -> List:
    try:
        out = client.app.bsky.feed.search_posts({"q": HASHTAG_QUERY, "sort": "latest", "limit": max_items})
        return getattr(out, "posts", []) or []
    except Exception:
        return []


def parse_at_uri_rkey(uri: str) -> Optional[Tuple[str, str, str]]:
    if not uri or not uri.startswith("at://"):
        return None
    parts = uri[len("at://"):].split("/")
    if len(parts) < 3:
        return None
    return parts[0], parts[1], parts[2]


def force_unrepost_unlike_if_needed(
    client: Client,
    me: str,
    subject_uri: str,
    repost_records: Dict[str, str],
    like_records: Dict[str, str],
):
    # unrepost
    if subject_uri in repost_records:
        existing_repost_uri = repost_records.get(subject_uri)
        parsed = parse_at_uri_rkey(existing_repost_uri) if existing_repost_uri else None
        if parsed:
            did, collection, rkey = parsed
            if did == me and collection == "app.bsky.feed.repost":
                try:
                    client.app.bsky.feed.repost.delete({"repo": me, "rkey": rkey})
                except Exception as e:
                    log(f"⚠️ unrepost failed: {e}")
        repost_records.pop(subject_uri, None)

    # unlike
    if subject_uri in like_records:
        existing_like_uri = like_records.get(subject_uri)
        parsed = parse_at_uri_rkey(existing_like_uri) if existing_like_uri else None
        if parsed:
            did, collection, rkey = parsed
            if did == me and collection == "app.bsky.feed.like":
                try:
                    client.app.bsky.feed.like.delete({"repo": me, "rkey": rkey})
                except Exception as e:
                    log(f"⚠️ unlike failed: {e}")
        like_records.pop(subject_uri, None)


def repost_and_like(
    client: Client,
    me: str,
    subject_uri: str,
    subject_cid: str,
    repost_records: Dict[str, str],
    like_records: Dict[str, str],
    force_refresh: bool,
) -> bool:
    # In hashtag-only mode is force_refresh standaard False,
    # maar we houden de functie template-compatibel.
    if force_refresh:
        force_unrepost_unlike_if_needed(client, me, subject_uri, repost_records, like_records)
    else:
        if subject_uri in repost_records:
            return False

    # repost
    try:
        out = client.app.bsky.feed.repost.create(
            repo=me,
            record={
                "subject": {"uri": subject_uri, "cid": subject_cid},
                "createdAt": utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        repost_uri = getattr(out, "uri", None)
        if repost_uri:
            repost_records[subject_uri] = repost_uri
    except Exception as e:
        log(f"⚠️ Repost error: {e}")
        return False

    # like
    try:
        out_like = client.app.bsky.feed.like.create(
            repo=me,
            record={
                "subject": {"uri": subject_uri, "cid": subject_cid},
                "createdAt": utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        like_uri = getattr(out_like, "uri", None)
        if like_uri:
            like_records[subject_uri] = like_uri
    except Exception as e:
        log(f"⚠️ Like error: {e}")

    return True


def main():
    log("=== BSKYPROMO HASHTAG BOT START ===")

    username = os.getenv(ENV_USERNAME, "").strip()
    password = os.getenv(ENV_PASSWORD, "").strip()
    if not username or not password:
        log(f"❌ Missing env {ENV_USERNAME} / {ENV_PASSWORD}")
        return

    cutoff = utcnow() - timedelta(hours=HOURS_BACK)

    state = load_state(STATE_FILE)
    repost_records: Dict[str, str] = state.get("repost_records", {})
    like_records: Dict[str, str] = state.get("like_records", {})

    client = Client()
    client.login(username, password)
    me = client.me.did
    log(f"✅ Logged in as {me}")

    # Template-compat logs (feeds/lijsten bestaan maar zijn leeg)
    log("Feeds to process: 0 (all empty)")
    log("Lists to process: 0 (all empty)")

    # Hashtag
    log(f"🔎 Hashtag search: {HASHTAG_QUERY}")
    posts = fetch_hashtag_posts(client, HASHTAG_MAX_ITEMS)
    log(f"Hashtag posts fetched: {len(posts)}")

    # Build candidates
    candidates: List[Dict] = []
    for post in posts:
        record = getattr(post, "record", None)
        if not record:
            continue

        if getattr(record, "reply", None):
            continue

        if is_quote_post(record):
            continue

        if not has_media(record):
            continue

        uri = getattr(post, "uri", None)
        cid = getattr(post, "cid", None)
        if not uri or not cid:
            continue

        created = parse_time(post)
        if not created or created < cutoff:
            continue

        author = getattr(post, "author", None)
        ah = (getattr(author, "handle", "") or "").lower()
        ad = (getattr(author, "did", "") or "").lower()

        candidates.append({
            "uri": uri,
            "cid": cid,
            "created": created,
            "author_key": ad or ah or uri,
            "force_refresh": False,
        })

    # Dedup + oldest-first
    seen: Set[str] = set()
    deduped: List[Dict] = []
    for c in sorted(candidates, key=lambda x: x["created"]):
        if c["uri"] in seen:
            continue
        seen.add(c["uri"])
        deduped.append(c)

    log(f"🧩 Candidates total (deduped): {len(deduped)}")

    total_done = 0
    per_user_count: Dict[str, int] = {}

    for c in deduped:
        if total_done >= MAX_PER_RUN:
            break

        ak = c["author_key"]
        per_user_count.setdefault(ak, 0)

        if per_user_count[ak] >= MAX_PER_USER:
            continue

        ok = repost_and_like(
            client, me, c["uri"], c["cid"],
            repost_records, like_records,
            force_refresh=False
        )
        if ok:
            total_done += 1
            per_user_count[ak] += 1
            log(f"✅ Repost+Like: {c['uri']}")
            time.sleep(SLEEP_SECONDS)

    state["repost_records"] = repost_records
    state["like_records"] = like_records
    save_state(STATE_FILE, state)
    log(f"🔥 Done — total reposts this run: {total_done}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("=== FATAL ERROR ===", flush=True)
        traceback.print_exc()
        raise