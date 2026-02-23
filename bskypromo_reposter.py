from atproto import Client
import os
import re
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Set

# ================== ENV CONFIG ==================

BSKY_USERNAME = os.getenv("BSKY_USERNAME")
BSKY_PASSWORD = os.getenv("BSKY_PASSWORD")

SEARCH_QUERY = os.getenv("SEARCH_QUERY", "#bskypromo")
SEARCH_LIMIT = int(os.getenv("SEARCH_LIMIT", "200"))          # hoeveel results we per run binnenhalen
MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "100"))            # max reposts per run
POST_DELAY_SECONDS = float(os.getenv("POST_DELAY_SECONDS", "1.2"))

HOURS_BACK = int(os.getenv("HOURS_BACK", "24"))               # max terugkijken
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "14"))

FOLLOW_LIST_LINK = os.getenv("FOLLOW_LIST_LINK", "")
LIST_MEMBER_LIMIT = int(os.getenv("LIST_MEMBER_LIMIT", "500"))

STATE_FILE = os.getenv("STATE_FILE", "state.json")

LIST_URL_RE = re.compile(r"https://bsky\.app/profile/([^/]+)/lists/([^/?#]+)", re.I)

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

def contains_hashtag(text: str, tag: str = "#bskypromo") -> bool:
    # case-insensitive, “losse hashtag” match
    return re.search(rf"(?i)(^|\s){re.escape(tag)}(\s|$|[!,.?:;])", text or "") is not None

def has_media(record) -> bool:
    """
    True als record een image/video embed heeft.
    """
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

# ================== FOLLOW (FROM LIST) ==================

def follow_list_members(client: Client, list_link: str, state: Dict[str, Any]) -> None:
    if not list_link:
        return

    list_uri = normalize_list_uri(client, list_link)
    if not list_uri:
        print("⚠️ FOLLOW_LIST_LINK kon niet worden genormalized. Check de link/uri.")
        return

    target_dids = fetch_list_members(client, list_uri, LIST_MEMBER_LIMIT)
    already_followed: Set[str] = set(state.get("followed", []))

    for did in target_dids:
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

# ================== REPOST / LIKE / CLEANUP ==================

def search_posts(client: Client, q: str, limit: int):
    return client.app.bsky.feed.search_posts({"q": q, "limit": limit})

def delete_repost_record(client: Client, repost_uri: str) -> bool:
    """
    repost_uri: at://did/app.bsky.feed.repost/rkey
    """
    try:
        if not repost_uri.startswith("at://"):
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
            # keep if we couldn't delete (so we can retry later)
            keep.append(item)

        time.sleep(0.15)

    state["reposts"] = keep
    return removed

def do_reposts_and_likes(client: Client, state: Dict[str, Any]) -> int:
    cutoff = now_dt() - timedelta(hours=HOURS_BACK)

    known_post_uris = {x.get("post_uri") for x in state.get("reposts", []) if x.get("post_uri")}
    out = search_posts(client, SEARCH_QUERY, SEARCH_LIMIT)
    posts = getattr(out, "posts", []) or []

    # search is vaak newest-first; wij doen oldest-first voor nette verwerking
    def created_dt(p) -> datetime:
        rec = getattr(p, "record", None)
        dt = parse_dt(getattr(rec, "createdAt", "")) or now_dt()
        return dt
    posts.sort(key=created_dt)

    made = 0
    for p in posts:
        if made >= MAX_PER_RUN:
            break

        post_uri = getattr(p, "uri", None)
        post_cid = getattr(p, "cid", None)
        if not post_uri or not post_cid:
            continue

        if post_uri in known_post_uris:
            continue

        record = getattr(p, "record", None)
        if not record:
            continue

        # 24h cutoff
        p_created = created_dt(p)
        if p_created < cutoff:
            continue

        # must contain hashtag AND must be media (replies toegestaan)
        txt = extract_post_text(p)
        if not contains_hashtag(txt, "#bskypromo"):
            continue
        if not has_media(record):
            continue

        try:
            ts = now_iso()

            # 🔁 repost
            created_out = client.app.bsky.feed.repost.create(
                repo=client.me.did,
                record={"subject": {"uri": post_uri, "cid": post_cid}, "createdAt": ts},
            )

            repost_uri = getattr(created_out, "uri", None)

            # ❤️ like
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
            print(f"✅ Reposted + liked: {post_uri}")
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

    # 1) follow list (jij beheert list)
    follow_list_members(client, FOLLOW_LIST_LINK, state)

    # 2) cleanup reposts older than 14 days
    removed = cleanup_old_reposts(client, state)

    # 3) repost + like media posts with #bskypromo from last 24 hours
    made = do_reposts_and_likes(client, state)

    save_state(STATE_FILE, state)
    print(f"🔥 Done — reposted+liked: {made}, cleaned: {removed}, tracked: {len(state.get('reposts', []))}")

if __name__ == "__main__":
    main()
