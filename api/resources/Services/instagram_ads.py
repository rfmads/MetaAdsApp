import json
import time
import uuid
import logging
from flask import Blueprint, g, request, jsonify
from threading import Thread
from db.db import execute, query_dict
from db.config_store import get_config

def format_instagram_to_dataslayer(rows):
    headers = [
        "User ID", "Username", "Name", "User image URL", "Date", 
        "Media ID", "Media permalink", "Media type", "Media created date", 
        "Media Product type", "Media shortcode", "Media URL", "Media Image", 
        "Media thumbnail URL (Video only)", "Media thumbnail", "Media caption"
    ]

    data = [headers]

    for r in rows:
        m_url = r.get("media_url")
        t_url = r.get("media_thumbnail_url")

        data.append([
            str(r.get("user_id")),  # FIX: Cast to string to match old data
            r.get("username"),
            r.get("name"),
            r.get("user_image_url"), 
            str(r.get("date")),
            str(r.get("media_id")), # FIX: Cast to string
            r.get("media_permalink"),
            r.get("media_type"),
            str(r.get("media_created_date")),
            r.get("media_product_type"),
            r.get("media_shortcode"),
            m_url,
            
            # Formats as =IMAGE("url") or "--" to match your old format style
            f'=IMAGE("{m_url}")' if m_url else '--', 
            
            t_url if t_url else '--', # Thumbnail URL
            
            f'=IMAGE("{t_url}")' if t_url else '=IMAGE("--")', # Thumbnail Image
            
            r.get("media_caption")
        ])

    return {"result": data}
# ca.link_url
def fetch_instagram_insights():
    return query_dict(""" 
SELECT 
    p.ig_user_id AS user_id,
    p.ig_username AS username,
    p.page_name AS name,
    NULL AS user_image_url,
    po.created_time AS date,
    po.post_id AS media_id,
    po.permalink_url AS media_permalink,
    po.media_type AS media_type,
    po.created_time AS media_created_date,
    po.media_type AS media_product_type,
    -- Simplified Shortcode logic
    SUBSTRING_INDEX(REPLACE(po.permalink_url, 'https://www.instagram.com/reel/', ''), '/', 1) AS media_shortcode,
    -- Return raw URLs only
    COALESCE(ca.link_url, po.thumbnail_url) AS media_url,
    po.thumbnail_url AS media_thumbnail_url,
    ca.body AS media_caption
FROM posts po
JOIN pages p ON p.page_id = po.page_id
LEFT JOIN ad_posts ap ON ap.post_row_id = po.id
LEFT JOIN ads a ON a.ad_id = ap.ad_id
LEFT JOIN creative_ads ca ON ca.creative_id = a.creative_id
WHERE po.created_time >= NOW() - INTERVAL 2 DAY
    AND po.platform = 'instagram'
ORDER BY po.created_time DESC

    """)