# services/ad_posts_service.py

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_dict, query_one, execute


# ============================================================
# Helpers: find post_row_id in posts table
# ============================================================

def _find_post_row_id_by_effective_story_id(story_id: str):
    if not story_id:
        return None
    return query_one(
        """
        SELECT id
        FROM posts
        WHERE effective_object_story_id = %s
        ORDER BY created_time DESC
        LIMIT 1
        """,
        (story_id,),
    )


def _find_post_row_id_by_instagram_permalink(url: str):
    if not url:
        return None
    return query_one(
        """
        SELECT id
        FROM posts
        WHERE instagram_permalink_url = %s
        ORDER BY created_time DESC
        LIMIT 1
        """,
        (url,),
    )


def _upsert_ad_post(ad_id: int, post_row_id: int, link_type: str) -> None:
    """
    ad_posts:
      - UNIQUE(ad_id)
      - FK(ad_id)->ads(ad_id)
      - FK(post_row_id)->posts(id)
    """
    execute(
        """
        INSERT INTO ad_posts (ad_id, post_row_id, link_type)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
          post_row_id = VALUES(post_row_id),
          link_type   = VALUES(link_type),
          created_at  = CURRENT_TIMESTAMP
        """,
        (ad_id, post_row_id, link_type),
    )


# ============================================================
# Main sync
# ============================================================

def sync_ad_posts(user_token: str, limit: int = 0) -> None:
    """
    Strategy:
    - Read ads from DB (ad_id + post_id).
    - If ads.post_id exists:
        A) if contains '_' => facebook effective_object_story_id (pageId_postId)
           match posts.effective_object_story_id
        B) else it might be IG permalink or IG media id (rare); we attempt match by
           instagram_permalink_url first if it looks like a URL.
    - If ads.post_id is empty:
        fallback: fetch ad creative and try to resolve:
          - creative.effective_object_story_id
          - creative.instagram_permalink_url
    """
    client = MetaGraphClient(user_token)

    ads = query_dict(
        """
        SELECT ad_id, post_id
        FROM ads
        ORDER BY ad_id DESC
        """
    )

    if limit and limit > 0:
        ads = ads[:limit]

    logger.info(f"Syncing ad_posts from ads={len(ads)}")

    saved = 0
    skipped = 0
    failed = 0

    # fields to fallback when post_id is null
    AD_FIELDS = "id,creative{effective_object_story_id,instagram_permalink_url}"

    for row in ads:
        ad_id = int(row["ad_id"])
        post_id_value = row.get("post_id")

        try:
            post_row_id = None
            link_type = None

            # ------------------------------------------------
            # 1) Prefer DB field ads.post_id
            # ------------------------------------------------
            if post_id_value:
                pid = str(post_id_value).strip()

                # Facebook effective_object_story_id usually "pageid_postid"
                if "_" in pid:
                    found = _find_post_row_id_by_effective_story_id(pid)
                    if found and found.get("id"):
                        post_row_id = int(found["id"])
                        link_type = "facebook_story"

                # If it looks like an IG url (rare stored in post_id)
                if not post_row_id and pid.startswith("http"):
                    found = _find_post_row_id_by_instagram_permalink(pid)
                    if found and found.get("id"):
                        post_row_id = int(found["id"])
                        link_type = "instagram_permalink"

            # ------------------------------------------------
            # 2) Fallback: call API ad -> creative
            # ------------------------------------------------
            if not post_row_id:
                ad_data = client.get(str(ad_id), params={"fields": AD_FIELDS}) or {}
                creative = ad_data.get("creative") or {}

                eff_story = creative.get("effective_object_story_id")
                ig_link = creative.get("instagram_permalink_url")

                if eff_story:
                    found = _find_post_row_id_by_effective_story_id(str(eff_story))
                    if found and found.get("id"):
                        post_row_id = int(found["id"])
                        link_type = "facebook_story"

                if not post_row_id and ig_link:
                    found = _find_post_row_id_by_instagram_permalink(str(ig_link))
                    if found and found.get("id"):
                        post_row_id = int(found["id"])
                        link_type = "instagram_permalink"

            # ------------------------------------------------
            # Save
            # ------------------------------------------------
            if not post_row_id:
                skipped += 1
                continue

            _upsert_ad_post(ad_id=ad_id, post_row_id=post_row_id, link_type=link_type or "facebook_story")
            saved += 1

        except Exception as e:
            failed += 1
            logger.error(f"⚠️ ad_posts failed for ad_id={ad_id}: {e}")

    logger.info(f"✅ ad_posts sync done. saved={saved}, skipped={skipped}, failed={failed}")
