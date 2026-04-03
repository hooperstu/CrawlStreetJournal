"""Seed a test project with rich crawl data for Playwright testing."""

import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import storage

SLUG = "bugbot-test"
RUN_NAME = "bugbot-run"

PAGES_FIELDS = storage.PAGES_FIELDS


def _make_page(
    url, domain, title, kind="webpage", cms="", author="", publisher="",
    json_ld_types="", microdata_types="", rdfa_types="",
    schema_price="", schema_currency="", schema_availability="",
    schema_rating="", schema_review_count="",
    schema_event_date="", schema_event_location="",
    schema_job_title="", schema_job_location="",
    schema_recipe_time="",
    hreflang="", feed_urls="", robots="", breadcrumb="",
    word_count=200, depth=0,
):
    return {
        "requested_url": url,
        "final_url": url,
        "domain": domain,
        "http_status": "200",
        "content_type": "text/html",
        "title": title,
        "meta_description": f"Description for {title}",
        "lang": "en",
        "canonical_url": url,
        "og_title": title,
        "og_type": "website",
        "og_description": f"OG desc for {title}",
        "twitter_card": "summary",
        "json_ld_types": json_ld_types,
        "tags_all": "test|sample",
        "url_content_hint": "",
        "content_kind_guess": kind,
        "h1_joined": title,
        "word_count": str(word_count),
        "http_last_modified": "",
        "etag": "",
        "sitemap_lastmod": "2025-03-01",
        "referrer_sitemap_url": "",
        "heading_outline": "H2:Overview|H3:Details",
        "date_published": "2025-01-15",
        "date_modified": "2025-03-01",
        "visible_dates": "15 January 2025",
        "link_count_internal": "12",
        "link_count_external": "3",
        "link_count_total": "15",
        "img_count": "4",
        "img_missing_alt_count": "1",
        "readability_fk_grade": "9.2",
        "privacy_policy_url": "/privacy",
        "analytics_signals": "googletagmanager.com|dataLayer",
        "training_related_flag": "",
        "nav_link_count": "8",
        "wcag_lang_valid": "1",
        "wcag_heading_order_valid": "1",
        "wcag_title_present": "1",
        "wcag_form_labels_pct": "1.0",
        "wcag_landmarks_present": "1",
        "wcag_vague_link_pct": "0.05",
        "author": author,
        "publisher": publisher,
        "json_ld_id": f"{url}#main" if json_ld_types else "",
        "cms_generator": cms,
        "robots_directives": robots,
        "hreflang_links": hreflang,
        "feed_urls": feed_urls,
        "pagination_next": "",
        "pagination_prev": "",
        "breadcrumb_schema": breadcrumb,
        "microdata_types": microdata_types,
        "rdfa_types": rdfa_types,
        "schema_price": schema_price,
        "schema_currency": schema_currency,
        "schema_availability": schema_availability,
        "schema_rating": schema_rating,
        "schema_review_count": schema_review_count,
        "schema_event_date": schema_event_date,
        "schema_event_location": schema_event_location,
        "schema_job_title": schema_job_title,
        "schema_job_location": schema_job_location,
        "schema_recipe_time": schema_recipe_time,
        "extraction_coverage_pct": "55.0",
        "referrer_url": "seed",
        "depth": str(depth),
        "discovered_at": "2025-03-15 10:00:00",
    }


def seed():
    project_dir = os.path.join(config.PROJECTS_DIR, SLUG)
    if os.path.isdir(project_dir):
        import shutil
        shutil.rmtree(project_dir)

    storage.create_project("BugBot Test", "Test project for Playwright testing")
    # The slug might differ, find it
    projects = storage.list_projects()
    slug = None
    for p in projects:
        if p["name"] == "BugBot Test":
            slug = p["slug"]
            break
    if not slug:
        slug = SLUG

    ctx = storage.activate_project(slug)
    run_folder = ctx.create_run(run_name=RUN_NAME)
    run_dir = os.path.join(ctx.output_dir, run_folder)

    pages = []

    # WordPress blog pages
    for i in range(15):
        pages.append(_make_page(
            f"https://blog.example.com/post-{i}", "blog.example.com",
            f"Blog Post {i}: Test Article", kind="blog",
            cms="WordPress 6.4", author=f"Author {i % 4}",
            publisher="Example Blog", json_ld_types="BlogPosting",
            feed_urls="/feed.xml", breadcrumb=f"Home > Blog > Post {i}",
            word_count=300 + i * 50, depth=i % 3,
        ))

    # Shopify product pages
    for i in range(10):
        pages.append(_make_page(
            f"https://shop.example.com/products/item-{i}", "shop.example.com",
            f"Product {i}: Widget", kind="product",
            cms="Shopify", publisher="Widget Store",
            json_ld_types="Product", microdata_types="Product",
            schema_price=str(9.99 + i * 5),
            schema_currency="GBP",
            schema_availability="InStock" if i % 3 != 0 else "OutOfStock",
            schema_rating=str(round(3.0 + i * 0.2, 1)),
            schema_review_count=str(10 + i * 15),
            word_count=150, depth=1,
        ))

    # Ghost news pages
    for i in range(8):
        pages.append(_make_page(
            f"https://news.example.com/article-{i}", "news.example.com",
            f"News Article {i}: Breaking Story", kind="news",
            cms="Ghost 5.0", author=f"Reporter {i % 3}",
            publisher="Example News", json_ld_types="NewsArticle",
            hreflang=f"fr=https://news.example.com/fr/article-{i}",
            robots="meta:index, follow",
            word_count=500 + i * 100, depth=1,
        ))

    # Event pages
    for i in range(5):
        pages.append(_make_page(
            f"https://events.example.com/event-{i}", "events.example.com",
            f"Community Event {i}", kind="event",
            cms="WordPress 6.4", json_ld_types="Event",
            schema_event_date=f"2025-{6 + i:02d}-15T18:00:00Z",
            schema_event_location=["Town Hall", "Convention Centre", "Online", "Library", "Park"][i],
            word_count=100, depth=1,
        ))

    # Job posting pages
    for i in range(6):
        titles = ["Software Engineer", "Designer", "Product Manager", "Data Analyst", "DevOps", "QA Lead"]
        locations = ["London", "Manchester", "Remote", "Edinburgh", "Bristol", "London"]
        pages.append(_make_page(
            f"https://careers.example.com/jobs/job-{i}", "careers.example.com",
            f"Job: {titles[i]}", kind="job_posting",
            json_ld_types="JobPosting",
            schema_job_title=titles[i],
            schema_job_location=locations[i],
            word_count=250, depth=1,
        ))

    # Recipe pages
    for i in range(4):
        pages.append(_make_page(
            f"https://food.example.com/recipes/recipe-{i}", "food.example.com",
            f"Recipe {i}: Delicious Dish", kind="recipe",
            json_ld_types="Recipe",
            schema_recipe_time=f"PT{30 + i * 15}M",
            word_count=400, depth=1,
        ))

    # Drupal gov pages with RDFa
    for i in range(5):
        pages.append(_make_page(
            f"https://gov.example.com/guidance/page-{i}", "gov.example.com",
            f"Government Guidance {i}", kind="guidance",
            cms="Drupal 10", rdfa_types="Article",
            robots="meta:index, follow|header:noarchive",
            breadcrumb=f"Home > Guidance > Page {i}",
            word_count=800, depth=2,
        ))

    # Pages with errors/edge cases
    pages.append({
        **_make_page(
            "https://broken.example.com/page", "broken.example.com",
            "", kind="unknown",
        ),
        "title": "",
        "http_status": "404",
        "word_count": "0",
        "extraction_coverage_pct": "5.0",
    })

    # Write pages CSV
    pages_path = os.path.join(run_dir, config.PAGES_CSV)
    with open(pages_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PAGES_FIELDS, quoting=csv.QUOTE_ALL,
                           extrasaction="ignore")
        w.writeheader()
        for page in pages:
            safe = {k: page.get(k, "") for k in PAGES_FIELDS}
            w.writerow(safe)

    # Write edges CSV
    edges_path = os.path.join(run_dir, config.EDGES_CSV)
    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=storage.EDGE_FIELDS, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for i, page in enumerate(pages[:20]):
            for j in range(i + 1, min(i + 3, len(pages))):
                w.writerow({
                    "from_url": page["final_url"],
                    "to_url": pages[j]["final_url"],
                    "link_text": f"Link to {pages[j]['title'][:30]}",
                    "discovered_at": "2025-03-15 10:00:00",
                })

    # Write tags CSV
    tags_path = os.path.join(run_dir, config.TAGS_CSV)
    with open(tags_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=storage.TAG_ROW_FIELDS, quoting=csv.QUOTE_ALL)
        w.writeheader()
        tag_values = ["technology", "health", "education", "finance", "sport",
                      "politics", "science", "culture", "environment", "business"]
        for i, page in enumerate(pages[:30]):
            for tag in tag_values[i % 5:(i % 5) + 3]:
                w.writerow({
                    "page_url": page["final_url"],
                    "tag_value": tag,
                    "tag_source": "meta:keywords",
                    "discovered_at": "2025-03-15 10:00:00",
                })

    # Write nav_links CSV
    nav_path = os.path.join(run_dir, config.NAV_LINKS_CSV)
    with open(nav_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=storage.NAV_LINK_FIELDS, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for page in pages[:10]:
            w.writerow({
                "page_url": page["final_url"],
                "nav_href": "/about",
                "nav_text": "About Us",
                "discovered_at": "2025-03-15 10:00:00",
            })

    # Write errors CSV
    errors_path = os.path.join(run_dir, config.ERRORS_CSV)
    with open(errors_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=storage.ERROR_FIELDS, quoting=csv.QUOTE_ALL)
        w.writeheader()
        w.writerow({
            "url": "https://broken.example.com/missing",
            "error_type": "fetch_failed",
            "message": "Connection refused",
            "http_status": "0",
            "discovered_at": "2025-03-15 10:00:00",
        })

    # Update state to completed
    storage.save_crawl_state(
        run_dir,
        status="completed",
        pages_crawled=len(pages),
        assets_from_pages=0,
        queue=[],
        started_at="2025-03-15 10:00:00",
        stopped_at="2025-03-15 10:05:00",
    )

    print(f"Seeded project '{slug}' with run '{run_folder}'")
    print(f"  {len(pages)} pages across {len(set(p['domain'] for p in pages))} domains")
    print(f"  Run dir: {run_dir}")
    return slug, run_folder


if __name__ == "__main__":
    slug, run_folder = seed()
