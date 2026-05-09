import json
import time
import uuid
import logging
from flask import Blueprint, g, request, jsonify
from threading import Thread
from db.db import execute, query_dict
from db.config_store import get_config

def safe_str(v):
    if v is None:
        return "--"
    return str(v)

def format_posts_to_dataslayer(rows):
    headers = [
        "Page name",
        "Page ID",
        "Date",
        "Post ID",
        "Video ID",
        "Video description",
        "Link to post",
        "Video source URL",
        "Video embed HTML",
        "Video image URL",
        "Post image URL",
        "Post image",
        "Post type",
        "Post name",
        "Post story",
        "Post description",
        "Post shared link",
        "Post object ID",
        "Post thumbnail URL",
        "Universal video ID",
        "Video title",
        "Video permalink URL"
    ]

    data = [headers]

    for r in rows:
        data.append([
            safe_str(r.get("page_name")),
            safe_str(r.get("page_id")),
            safe_str(r.get("date")),
            safe_str(r.get("post_id")),

            safe_str(r.get("video_id")),
            safe_str(r.get("video_description")),

            safe_str(r.get("link_to_post")),

            safe_str(r.get("video_source_url")),
            safe_str(r.get("video_embed_html")),
            safe_str(r.get("video_image_url")),

            safe_str(r.get("post_image_url")),
            safe_str(r.get("post_image")),

            safe_str(r.get("post_type")),

            safe_str(r.get("post_name")),
            safe_str(r.get("post_story")),
            safe_str(r.get("post_description")),

            safe_str(r.get("post_shared_link")),
            safe_str(r.get("post_object_id")),
            safe_str(r.get("post_thumbnail_url")),

            safe_str(r.get("universal_video_id")),
            safe_str(r.get("video_title")),
            safe_str(r.get("video_permalink_url")),
        ])

    return {"result": data}

def fetch_facebook_insights():
    return query_dict(""" 
      SELECT 
    p.page_name AS page_name,
    p.page_id AS page_id,
    po.created_time AS date,
    po.post_id AS post_id,
    ca.video_id AS video_id,
    ca.body AS video_description,
    po.permalink_url AS link_to_post,
    ca.link_url AS video_source_url,
    ca.instagram_permalink_url AS video_embed_html,
    ca.thumbnail_url AS video_image_url,
    po.thumbnail_url AS post_image_url,
    po.thumbnail_url AS post_image,
    po.media_type AS post_type,
    ca.name AS post_name,
    ca.body AS post_story,
    ca.body AS post_description,
    po.permalink_url AS post_shared_link,
    po.effective_object_story_id AS post_object_id,
    po.thumbnail_url AS post_thumbnail_url,
    ca.video_id AS universal_video_id,
    ca.name AS video_title,
    po.permalink_url AS video_permalink_url
FROM
    posts po
        JOIN
    pages p ON p.page_id = po.page_id
        LEFT JOIN
    ad_posts ap ON ap.post_row_id = po.id
        LEFT JOIN
    ads a ON a.ad_id = ap.ad_id
        LEFT JOIN
    creative_ads ca ON ca.creative_id = a.creative_id
WHERE
    po.created_time >= NOW() - INTERVAL 30 DAY
        AND ca.video_id IS NOT NULL
        AND po.platform = 'facebook'
 AND po.created_time >= CURDATE() - INTERVAL 2 DAY

    """)