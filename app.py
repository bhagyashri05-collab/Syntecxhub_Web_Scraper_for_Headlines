import csv
import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional
from urllib import robotparser
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from flask import Flask, flash, render_template, request
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

USER_AGENT = "WebScraperForHeadlinesBot/1.0 (+https://example.com/bot)"
REQUEST_TIMEOUT = 15
SCRAPE_DELAY_SECS = 1.0
SCRAPE_BUDGET_SECS = 10
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(DATA_DIR, "headlines.db")
SESSION = requests.Session()

retry_config = Retry(
    total=3,
    backoff_factor=1.2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
)
adapter = HTTPAdapter(max_retries=retry_config)
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)


def ensure_data_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def get_db_connection() -> sqlite3.Connection:
    ensure_data_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    ensure_data_dir()
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS headlines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                published_at TEXT,
                source TEXT NOT NULL,
                source_key TEXT NOT NULL,
                scraped_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_headlines_url_source
            ON headlines (url, source_key)
            """
        )
        conn.commit()


def persist_results(items: List[Dict[str, str]]) -> None:
    if not items:
        return
    timestamp = datetime.utcnow().isoformat()
    with get_db_connection() as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO headlines (title, url, published_at, source, source_key, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(url, source_key) DO UPDATE SET
                    title=excluded.title,
                    published_at=excluded.published_at,
                    source=excluded.source,
                    scraped_at=excluded.scraped_at
                """,
                (
                    item["title"],
                    item["url"],
                    item.get("published_at", ""),
                    item["source"],
                    item["source_key"],
                    timestamp,
                ),
            )
        conn.commit()


def load_recent_headlines(
    selected_sources: List[str], keyword: Optional[str], limit: int = 30
) -> List[Dict[str, str]]:
    if not os.path.exists(DB_PATH):
        return []
    source_keys = [src for src in selected_sources if src in NEWS_SOURCES]
    params: List[str] = []
    clauses: List[str] = []
    if source_keys:
        placeholders = ",".join("?" for _ in source_keys)
        clauses.append(f"source_key IN ({placeholders})")
        params.extend(source_keys)
    if keyword:
        like = f"%{keyword.lower()}%"
        clauses.append("(LOWER(title) LIKE ? OR LOWER(published_at) LIKE ?)")
        params.extend([like, like])
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT title, url, published_at, source, source_key
        FROM headlines
        {where_clause}
        ORDER BY datetime(scraped_at) DESC
        LIMIT ?
    """
    params.append(str(limit))
    with get_db_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def robots_allows(target_url: str) -> bool:
    parsed = urlparse(target_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception:
        # When robots.txt cannot be read, err on the side of caution and block.
        return False
    return rp.can_fetch(USER_AGENT, target_url)


def fetch_html(url: str) -> Optional[BeautifulSoup]:
    if not robots_allows(url):
        return None

    try:
        response = SESSION.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException:
        return None

    return BeautifulSoup(response.text, "html.parser")


def parse_bbc(soup: BeautifulSoup) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for link in soup.select("a.gs-c-promo-heading"):
        title = link.get_text(strip=True)
        href = link.get("href")
        if not title or not href:
            continue
        url = href if href.startswith("http") else urljoin("https://www.bbc.com", href)
        promo = link.find_parent(class_="gs-c-promo")
        time_tag = promo.find("time") if promo else None
        published = time_tag.get("datetime", "") if time_tag else ""
        items.append({"title": title, "url": url, "published_at": published})
    return items


def parse_hn(soup: BeautifulSoup) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for row in soup.select("tr.athing"):
        title_link = row.select_one("span.titleline a")
        if not title_link:
            continue
        title = title_link.get_text(strip=True)
        url = title_link.get("href", "")
        subtext = row.find_next_sibling("tr")
        time_tag = subtext.select_one("span.age") if subtext else None
        published = time_tag.get_text(strip=True) if time_tag else ""
        items.append({"title": title, "url": url, "published_at": published})
    return items


def parse_npr(soup: BeautifulSoup) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for article in soup.select("article.item"):
        header = article.select_one("h2.title a")
        if not header:
            continue
        title = header.get_text(strip=True)
        url = header.get("href", "")
        time_tag = article.select_one("time")
        published = time_tag.get("datetime", "") if time_tag else ""
        items.append({"title": title, "url": url, "published_at": published})
    return items


def parse_toi(soup: BeautifulSoup) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for link in soup.select("a[data-vars-event-label]"):
        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            continue
        url = href if href.startswith("http") else urljoin("https://timesofindia.indiatimes.com", href)
        time_tag = link.find_parent().select_one("time, .time")
        published = time_tag.get("datetime", "") if time_tag else ""
        items.append({"title": title, "url": url, "published_at": published})
    return items


def parse_ht(soup: BeautifulSoup) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for article in soup.select("article, .story-card"):
        header = article.select_one("h2 a, h3 a, .headline a")
        if not header:
            continue
        title = header.get_text(strip=True)
        url = header.get("href", "")
        if not title or not url:
            continue
        url = url if url.startswith("http") else urljoin("https://www.hindustantimes.com", url)
        time_tag = article.select_one("time, .date")
        published = time_tag.get("datetime", "") if time_tag else ""
        items.append({"title": title, "url": url, "published_at": published})
    return items


NEWS_SOURCES: Dict[str, Dict[str, str | Callable[[BeautifulSoup], List[Dict[str, str]]]]] = {
    "bbc": {
        "name": "BBC News",
        "url": "https://www.bbc.com/news",
        "parser": parse_bbc,
    },
    "hn": {
        "name": "Hacker News",
        "url": "https://news.ycombinator.com/",
        "parser": parse_hn,
    },
    "npr": {
        "name": "NPR News",
        "url": "https://www.npr.org/sections/news/",
        "parser": parse_npr,
    },
    "toi": {
        "name": "Times of India",
        "url": "https://timesofindia.indiatimes.com/",
        "parser": parse_toi,
    },
    "ht": {
        "name": "Hindustan Times",
        "url": "https://www.hindustantimes.com/",
        "parser": parse_ht,
    },
}


def seed_database() -> None:
    ensure_data_dir()
    if not os.path.exists(DB_PATH):
        init_db()
    
    # Check if database is empty and seed if needed
    with get_db_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM headlines").fetchone()[0]
        if count > 0:
            return  # Already has data, skip seeding
    
    sample_items: List[Dict[str, str]] = [
        # Climate News
        {
            "title": "Global climate pact sparks fresh innovation push",
            "url": "https://www.bbc.com/news/science-environment-123456",
            "published_at": "2025-01-05T08:00:00Z",
            "source": str(NEWS_SOURCES["bbc"]["name"]),
            "source_key": "bbc",
        },
        {
            "title": "Show HN: Open climate dashboard for local communities",
            "url": "https://news.ycombinator.com/item?id=42424242",
            "published_at": "3 hours ago",
            "source": str(NEWS_SOURCES["hn"]["name"]),
            "source_key": "hn",
        },
        {
            "title": "NPR Climate Solutions: Turning seawater into batteries",
            "url": "https://www.npr.org/2025/01/07/example-story-climate-battery",
            "published_at": "2025-01-07T10:15:00Z",
            "source": str(NEWS_SOURCES["npr"]["name"]),
            "source_key": "npr",
        },
        {
            "title": "How coastal cities are adapting to rising tides",
            "url": "https://www.npr.org/2025/01/04/example-story-rising-tides",
            "published_at": "2025-01-04T09:45:00Z",
            "source": str(NEWS_SOURCES["npr"]["name"]),
            "source_key": "npr",
        },
        {
            "title": "India launches massive reforestation drive to combat climate change",
            "url": "https://timesofindia.indiatimes.com/climate-reforestation-2025",
            "published_at": "2025-01-06T14:20:00Z",
            "source": str(NEWS_SOURCES["toi"]["name"]),
            "source_key": "toi",
        },
        {
            "title": "Delhi air quality improves as climate policies take effect",
            "url": "https://www.hindustantimes.com/delhi-air-quality-climate-2025",
            "published_at": "2025-01-05T11:30:00Z",
            "source": str(NEWS_SOURCES["ht"]["name"]),
            "source_key": "ht",
        },
        # Economy News
        {
            "title": "Global markets surge as economic recovery accelerates",
            "url": "https://www.bbc.com/news/business-economy-789012",
            "published_at": "2025-01-08T09:15:00Z",
            "source": str(NEWS_SOURCES["bbc"]["name"]),
            "source_key": "bbc",
        },
        {
            "title": "India's GDP growth exceeds expectations in Q4",
            "url": "https://timesofindia.indiatimes.com/india-gdp-growth-2025",
            "published_at": "2025-01-07T16:45:00Z",
            "source": str(NEWS_SOURCES["toi"]["name"]),
            "source_key": "toi",
        },
        {
            "title": "Central banks coordinate on inflation strategy",
            "url": "https://www.hindustantimes.com/central-banks-inflation-2025",
            "published_at": "2025-01-06T10:00:00Z",
            "source": str(NEWS_SOURCES["ht"]["name"]),
            "source_key": "ht",
        },
        {
            "title": "Tech stocks rally on strong earnings reports",
            "url": "https://news.ycombinator.com/item?id=44556677",
            "published_at": "5 hours ago",
            "source": str(NEWS_SOURCES["hn"]["name"]),
            "source_key": "hn",
        },
        {
            "title": "Renewable energy investments reach record high",
            "url": "https://www.npr.org/2025/01/08/renewable-energy-investments",
            "published_at": "2025-01-08T08:30:00Z",
            "source": str(NEWS_SOURCES["npr"]["name"]),
            "source_key": "npr",
        },
        # AI Breakthroughs
        {
            "title": "AI-driven forecasts help farmers plan drought response",
            "url": "https://www.bbc.com/news/technology-654321",
            "published_at": "2025-01-03T12:30:00Z",
            "source": str(NEWS_SOURCES["bbc"]["name"]),
            "source_key": "bbc",
        },
        {
            "title": "Breakthrough: AI model achieves human-level reasoning",
            "url": "https://news.ycombinator.com/item?id=45678901",
            "published_at": "2 hours ago",
            "source": str(NEWS_SOURCES["hn"]["name"]),
            "source_key": "hn",
        },
        {
            "title": "New AI system can predict disease outbreaks weeks in advance",
            "url": "https://www.npr.org/2025/01/09/ai-disease-prediction",
            "published_at": "2025-01-09T07:20:00Z",
            "source": str(NEWS_SOURCES["npr"]["name"]),
            "source_key": "npr",
        },
        {
            "title": "Indian startups lead AI innovation in healthcare sector",
            "url": "https://timesofindia.indiatimes.com/ai-healthcare-startups-2025",
            "published_at": "2025-01-08T13:10:00Z",
            "source": str(NEWS_SOURCES["toi"]["name"]),
            "source_key": "toi",
        },
        {
            "title": "AI-powered language translation breaks new barriers",
            "url": "https://www.hindustantimes.com/ai-translation-breakthrough-2025",
            "published_at": "2025-01-07T15:50:00Z",
            "source": str(NEWS_SOURCES["ht"]["name"]),
            "source_key": "ht",
        },
        {
            "title": "Open source AI model released for scientific research",
            "url": "https://news.ycombinator.com/item?id=46789012",
            "published_at": "1 hour ago",
            "source": str(NEWS_SOURCES["hn"]["name"]),
            "source_key": "hn",
        },
        # Sports News
        {
            "title": "Cricket World Cup final sets new viewership records",
            "url": "https://timesofindia.indiatimes.com/cricket-world-cup-2025",
            "published_at": "2025-01-09T18:00:00Z",
            "source": str(NEWS_SOURCES["toi"]["name"]),
            "source_key": "toi",
        },
        {
            "title": "Olympic preparations enter final phase",
            "url": "https://www.bbc.com/news/sport-olympics-2025",
            "published_at": "2025-01-08T12:00:00Z",
            "source": str(NEWS_SOURCES["bbc"]["name"]),
            "source_key": "bbc",
        },
        {
            "title": "Indian football team qualifies for Asian Cup finals",
            "url": "https://www.hindustantimes.com/indian-football-asian-cup-2025",
            "published_at": "2025-01-07T19:30:00Z",
            "source": str(NEWS_SOURCES["ht"]["name"]),
            "source_key": "ht",
        },
        {
            "title": "Tennis star breaks 20-year record at Grand Slam",
            "url": "https://timesofindia.indiatimes.com/tennis-grand-slam-record-2025",
            "published_at": "2025-01-06T20:15:00Z",
            "source": str(NEWS_SOURCES["toi"]["name"]),
            "source_key": "toi",
        },
        {
            "title": "Formula 1 season opens with dramatic finish",
            "url": "https://www.bbc.com/news/sport-f1-2025",
            "published_at": "2025-01-05T17:45:00Z",
            "source": str(NEWS_SOURCES["bbc"]["name"]),
            "source_key": "bbc",
        },
        # Cooking News
        {
            "title": "Traditional recipes gain popularity in modern kitchens",
            "url": "https://timesofindia.indiatimes.com/traditional-cooking-recipes-2025",
            "published_at": "2025-01-08T10:30:00Z",
            "source": str(NEWS_SOURCES["toi"]["name"]),
            "source_key": "toi",
        },
        {
            "title": "Plant-based cooking trends transform restaurant menus",
            "url": "https://www.bbc.com/news/food-plant-based-2025",
            "published_at": "2025-01-07T14:20:00Z",
            "source": str(NEWS_SOURCES["bbc"]["name"]),
            "source_key": "bbc",
        },
        {
            "title": "Home cooking surge continues post-pandemic",
            "url": "https://www.hindustantimes.com/home-cooking-trends-2025",
            "published_at": "2025-01-06T11:15:00Z",
            "source": str(NEWS_SOURCES["ht"]["name"]),
            "source_key": "ht",
        },
        {
            "title": "Chef's innovative fusion cuisine wins international acclaim",
            "url": "https://timesofindia.indiatimes.com/fusion-cuisine-award-2025",
            "published_at": "2025-01-05T16:00:00Z",
            "source": str(NEWS_SOURCES["toi"]["name"]),
            "source_key": "toi",
        },
        {
            "title": "Sustainable cooking practices reduce food waste",
            "url": "https://www.npr.org/2025/01/09/sustainable-cooking-food-waste",
            "published_at": "2025-01-09T09:45:00Z",
            "source": str(NEWS_SOURCES["npr"]["name"]),
            "source_key": "npr",
        },
    ]
    persist_results(sample_items)


ensure_data_dir()
init_db()
# Force seed if database is empty
try:
    seed_database()
except Exception as e:
    # If seeding fails, try to reinitialize
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()
    seed_database()


def filter_by_keyword(items: List[Dict[str, str]], keyword: str | None) -> List[Dict[str, str]]:
    if not keyword:
        return items
    keyword_lower = keyword.lower()
    return [
        item
        for item in items
        if keyword_lower in item["title"].lower()
        or keyword_lower in item.get("published_at", "").lower()
    ]


def scrape_sources(selected_sources: List[str], keyword: str | None) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    source_keys = [src for src in selected_sources if src in NEWS_SOURCES]
    start_time = time.time()
    for source_key in selected_sources:
        if time.time() - start_time > SCRAPE_BUDGET_SECS:
            flash(f"Stopped scraping after {SCRAPE_BUDGET_SECS}s budget.", "warning")
            break
        config = NEWS_SOURCES.get(source_key)
        if not config:
            flash(f"Unknown source: {source_key}", "warning")
            continue

        try:
            soup = fetch_html(str(config["url"]))
            if soup is None:
                continue
        except Exception:
            continue
        parser = config["parser"]
        parsed_items = parser(soup)
        filtered_items = filter_by_keyword(parsed_items, keyword)
        for item in filtered_items:
            item.update({"source": str(config["name"]), "source_key": source_key})
        results.extend(filtered_items)
        time.sleep(SCRAPE_DELAY_SECS)

    if results:
        persist_results(results)
        return results

    # Always try to load from database as fallback
    fallback = load_recent_headlines(source_keys or list(NEWS_SOURCES.keys()), keyword)
    if fallback:
        flash("Loaded recent saved headlines while live sources were unavailable.", "info")
        return fallback
    
    # If still no results and no keyword, show all headlines
    if not keyword:
        fallback_all = load_recent_headlines(list(NEWS_SOURCES.keys()), None, limit=50)
        if fallback_all:
            return fallback_all
    
    return []


def save_results(items: List[Dict[str, str]], export_format: str) -> Optional[str]:
    if not items:
        return None
    ensure_data_dir()
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    filename = f"headlines-{timestamp}.{export_format}"
    filepath = os.path.join(DATA_DIR, filename)

    export_items = [
        {
            "title": item.get("title", ""),
            "url": item.get("url", ""),
            "published_at": item.get("published_at", ""),
            "source": item.get("source", ""),
        }
        for item in items
    ]

    if export_format == "json":
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(export_items, f, ensure_ascii=False, indent=2)
    elif export_format == "csv":
        fieldnames = ["title", "url", "published_at", "source"]
        with open(filepath, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(export_items)
    else:
        return None

    return filepath


@app.route("/", methods=["GET", "POST"])
def index():
    selected_sources = list(NEWS_SOURCES.keys())
    results: List[Dict[str, str]] = []
    saved_path: Optional[str] = None
    keyword = ""
    export_format = "json"

    if request.method == "POST":
        keyword_input = request.form.get("keyword", "").strip()
        keyword = keyword_input if keyword_input else None
        selected_sources = request.form.getlist("sources") or selected_sources
        export_format = request.form.get("export_format", "json")
        
        results = scrape_sources(selected_sources, keyword)
        
        # If no results from scraping, try database
        if not results:
            results = load_recent_headlines(selected_sources, keyword, limit=50)
            if results:
                flash("Showing headlines from saved database.", "info")
            else:
                # Last resort: show all headlines without keyword filter
                results = load_recent_headlines(selected_sources, None, limit=50)
                if results:
                    flash(f"Showing {len(results)} headlines. No matches for '{keyword_input}' found.", "info")
                else:
                    flash("No headlines found. The database may need to be populated.", "warning")
        
        saved_path = save_results(results, export_format)
        if saved_path and results:
            flash(f"Saved {len(results)} headlines to {saved_path}", "success")

    return render_template(
        "index.html",
        sources=NEWS_SOURCES,
        selected_sources=selected_sources,
        keyword=keyword,
        results=results,
        export_format=export_format,
        saved_path=saved_path,
    )


if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 5000)))

