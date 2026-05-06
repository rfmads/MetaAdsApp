import json
import time
import uuid
import logging
from flask import Blueprint, g, request, jsonify
from threading import Thread
from db.db import execute, query_dict
from db.config_store import get_config

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
            r.get("page_name"),
            str(r.get("page_id")),
            str(r.get("date")),
            r.get("post_id"),

            r.get("video_id"),
            r.get("video_description"),

            r.get("link_to_post"),

            r.get("video_source_url"),
            r.get("video_embed_html"),
            r.get("video_image_url"),

            r.get("post_image_url"),
            r.get("post_image"),

            r.get("post_type"),

            r.get("post_name"),
            r.get("post_story"),
            r.get("post_description"),

            r.get("post_shared_link"),
            r.get("post_object_id"),
            r.get("post_thumbnail_url"),

            r.get("universal_video_id"),
            r.get("video_title"),
            r.get("video_permalink_url"),
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
FROM posts po
JOIN pages p ON p.page_id = po.page_id
LEFT JOIN ad_posts ap ON ap.post_row_id = po.id
LEFT JOIN ads a ON a.ad_id = ap.ad_id
LEFT JOIN creative_ads ca ON ca.creative_id = a.creative_id
    """)