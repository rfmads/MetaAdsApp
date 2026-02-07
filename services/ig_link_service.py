from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_dict, execute

FIELDS = "instagram_business_account{id,username}"

def sync_pages_ig_link(user_token: str) -> None:
    rows = query_dict("SELECT page_id, page_access_token FROM pages")
    logger.info(f"Syncing IG link for pages={len(rows)}")

    updated, skipped, failed = 0, 0, 0

    for r in rows:
        page_id = str(r["page_id"])
        token = (r.get("page_access_token") or "").strip() or user_token
        client = MetaGraphClient(token)

        try:
            data = client.get(page_id, params={"fields": FIELDS})
            ig = data.get("instagram_business_account")

            if not ig or not ig.get("id"):
                skipped += 1
                continue

            ig_user_id = str(ig["id"])
            ig_username = ig.get("username")

            execute(
                """
                UPDATE pages
                SET ig_user_id=%s, ig_username=%s, updated_at=NOW()
                WHERE page_id=%s
                """,
                (ig_user_id, ig_username, page_id)
            )

            updated += 1

        except Exception as e:
            failed += 1
            logger.error(f"⚠️ IG link failed for page_id={page_id}: {e}")

    logger.info(f"✅ IG link sync done. updated={updated}, skipped={skipped}, failed={failed}")
