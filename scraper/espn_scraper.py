#!/usr/bin/env python3
"""
Pit Wall — ESPN F1 News Scraper
Runs 5x daily on Render. Respects robots.txt. GPT-4o-mini reformats articles.
Stores to Supabase (Render) or news.json (local fallback).

Usage:
  python espn_scraper.py          # normal run
  python espn_scraper.py --dry    # fetch + GPT, write nothing
"""

import os
import sys
import json
import time
import hashlib
import logging
import random
from datetime import datetime, timezone
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
ESPN_F1_URL  = 'https://www.espn.com/f1/'
BASE_URL     = 'https://www.espn.com'
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
SEEN_FILE    = os.path.join(SCRIPT_DIR, 'seen_articles.json')   # local only
NEWS_OUTPUT  = os.path.join(SCRIPT_DIR, '..', 'news.json')       # local only
MAX_OUTPUT   = 5
MAX_PER_RUN  = 8
MIN_BODY_LEN = 120
DELAY_RANGE  = (3.5, 7.0)
TIMEOUT      = 15
USER_AGENT   = (
    'PitWallBot/1.0 '
    '(personal F1 dashboard; non-commercial)'
)

DRY_RUN        = '--dry' in sys.argv
USE_SUPABASE   = bool(os.getenv('SUPABASE_URL') and os.getenv('SUPABASE_KEY'))

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('pitwall')

# ─────────────────────────────────────────────────────────────────────────────
# Supabase client (lazy init)
# ─────────────────────────────────────────────────────────────────────────────
_sb = None

def supabase():
    global _sb
    if _sb is None:
        from supabase import create_client
        _sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
    return _sb

def supabase_seen_urls() -> set[str]:
    """Return set of URLs already in pitwall_news table."""
    try:
        res = supabase().table('pitwall_news').select('url').execute()
        return {row['url'] for row in res.data if row.get('url')}
    except Exception as exc:
        log.error(f'Supabase fetch seen failed: {exc}')
        return set()

def supabase_insert(entry: dict) -> bool:
    try:
        supabase().table('pitwall_news').insert(entry).execute()
        return True
    except Exception as exc:
        log.error(f'Supabase insert failed: {exc}')
        return False

def supabase_trim() -> None:
    """Keep only the latest MAX_OUTPUT rows."""
    try:
        res = supabase().table('pitwall_news') \
            .select('id') \
            .order('scraped_at', desc=True) \
            .execute()
        ids_to_delete = [row['id'] for row in res.data[MAX_OUTPUT:]]
        if ids_to_delete:
            supabase().table('pitwall_news') \
                .delete() \
                .in_('id', ids_to_delete) \
                .execute()
            log.info(f'Trimmed {len(ids_to_delete)} old row(s) from Supabase')
    except Exception as exc:
        log.error(f'Supabase trim failed: {exc}')

# ─────────────────────────────────────────────────────────────────────────────
# robots.txt
# ─────────────────────────────────────────────────────────────────────────────
def load_robots(base: str) -> RobotFileParser:
    rp = RobotFileParser()
    rp.set_url(urljoin(base, '/robots.txt'))
    try:
        rp.read()
        log.info('robots.txt loaded')
        d = rp.crawl_delay(USER_AGENT)
        if d:
            log.info(f'  crawl-delay: {d}s')
    except Exception as exc:
        log.warning(f'robots.txt unavailable: {exc} — using conservative delays')
    return rp

def allowed(rp: RobotFileParser, url: str) -> bool:
    ok = rp.can_fetch(USER_AGENT, url)
    if not ok:
        log.warning(f'robots.txt blocks: {url}')
    return ok

def crawl_delay(rp: RobotFileParser) -> float:
    d = rp.crawl_delay(USER_AGENT) or rp.crawl_delay('*')
    return max(float(d), DELAY_RANGE[0]) if d else random.uniform(*DELAY_RANGE)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent':      USER_AGENT,
    'Accept':          'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
})

def safe_get(url: str, rp: RobotFileParser) -> requests.Response | None:
    if not allowed(rp, url):
        return None
    time.sleep(crawl_delay(rp))
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except requests.HTTPError as exc:
        log.error(f'HTTP {exc.response.status_code}: {url}')
    except requests.RequestException as exc:
        log.error(f'Request failed: {exc}')
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Local seen-tracking (only used when Supabase not available)
# ─────────────────────────────────────────────────────────────────────────────
def load_seen_local() -> set[str]:
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE) as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()

def save_seen_local(seen: set[str]) -> None:
    with open(SEEN_FILE, 'w') as f:
        json.dump(list(seen)[-500:], f)

def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:20]

# ─────────────────────────────────────────────────────────────────────────────
# ESPN scraping
# ─────────────────────────────────────────────────────────────────────────────
def get_article_links(rp: RobotFileParser) -> list[dict]:
    resp = safe_get(ESPN_F1_URL, rp)
    if not resp:
        return []

    soup  = BeautifulSoup(resp.text, 'html.parser')
    found = {}

    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if href.startswith('/'):
            href = urljoin(BASE_URL, href)
        if 'espn.com' not in href:
            continue
        if not any(p in href for p in ('/story/', '/news/')):
            continue
        if any(p in href for p in ('/video/', '/watch/', '/scores/')):
            continue
        if href in found:
            continue
        text = a.get_text(separator=' ', strip=True)
        if len(text) < 20:
            continue
        found[href] = text

    log.info(f'Found {len(found)} candidate article(s)')
    return [{'url': u, 'title': t} for u, t in found.items()][:MAX_PER_RUN]


def get_article_body(url: str, rp: RobotFileParser) -> str | None:
    resp = safe_get(url, rp)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, 'html.parser')

    for node in [
        soup.find('div', attrs={'data-testid': 'article-body'}),
        soup.find('div', class_=lambda c: c and 'article-body' in c),
        soup.find('div', class_=lambda c: c and 'story-body' in c),
        soup.find('article'),
    ]:
        if node:
            text = ' '.join(p.get_text(' ', strip=True) for p in node.find_all('p'))
            if len(text) >= MIN_BODY_LEN:
                return text[:4000]

    # Fallback
    text = ' '.join(
        p.get_text(' ', strip=True)
        for p in soup.find_all('p')
        if len(p.get_text(strip=True)) > 40
    )
    return text[:4000] if len(text) >= MIN_BODY_LEN else None

# ─────────────────────────────────────────────────────────────────────────────
# GPT
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """\
You are a senior F1 journalist writing for "Pit Wall" — a personal, insider-grade F1 dashboard.

Your task: take raw ESPN article content and rewrite it in a precise editorial format.

RULES
─────
headline  — Present-tense. Punchy. Max 10 words. No filler ("F1:", "GP:", etc.).
            Must convey the single most important fact of the article.
            Think front page of a quality newspaper.

summary   — Exactly 2–3 sentences. Dense with facts (names, numbers, context).
            Reads like you're briefing a busy F1 insider who is time-poor.
            No fluff, no "In this article…".

tag       — One short category label, max 2 words. Choose the most fitting:
            The Story · Engine Wars · Paddock · Transfer · Contract · Penalty ·
            Technical · Calendar · Race Day · Qualifying · Team Orders · Debut ·
            Regulation · Incident · Strategy · Safety

Respond ONLY with valid JSON, no markdown:
{"tag": "...", "headline": "...", "summary": "..."}\
"""

def gpt_format(title: str, body: str) -> dict | None:
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    try:
        resp = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user',   'content': f'Title: {title}\n\nArticle:\n{body}'},
            ],
            temperature=0.35,
            max_tokens=320,
            response_format={'type': 'json_object'},
        )
        data = json.loads(resp.choices[0].message.content.strip())
        for key in ('tag', 'headline', 'summary'):
            if not data.get(key):
                log.error(f'GPT missing key: {key}')
                return None
        return data
    except Exception as exc:
        log.error(f'GPT error: {exc}')
        return None

# ─────────────────────────────────────────────────────────────────────────────
# Local JSON fallback
# ─────────────────────────────────────────────────────────────────────────────
def load_news_local() -> list:
    path = os.path.normpath(NEWS_OUTPUT)
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_news_local(news: list) -> None:
    path = os.path.normpath(NEWS_OUTPUT)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(news[:MAX_OUTPUT], f, indent=2, ensure_ascii=False)
    log.info(f'news.json saved ({len(news[:MAX_OUTPUT])} articles)')

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def run() -> None:
    mode = 'Supabase' if USE_SUPABASE else 'local JSON'
    log.info(f'══ Pit Wall Scraper  [{mode}]{" [DRY RUN]" if DRY_RUN else ""} ══')

    if not os.getenv('OPENAI_API_KEY'):
        log.error('OPENAI_API_KEY not set — aborting')
        sys.exit(1)

    rp        = load_robots(BASE_URL)
    new_count = 0

    # ── Seen-URL tracking ──
    if USE_SUPABASE:
        seen_urls = supabase_seen_urls()   # full URLs from Supabase
    else:
        seen_hashes  = load_seen_local()
        current_news = load_news_local()

    links = get_article_links(rp)
    if not links:
        log.warning('No links found — ESPN page structure may have changed')

    for article in links:
        url = article['url']

        # Duplicate check
        if USE_SUPABASE:
            if url in seen_urls:
                log.debug(f'  skip (seen): {url}')
                continue
        else:
            h = url_hash(url)
            if h in seen_hashes:
                log.debug(f'  skip (seen): {url}')
                continue

        log.info(f'New article: {url}')

        body = get_article_body(url, rp)
        if not body:
            log.warning('  no body extracted — skip')
            if not USE_SUPABASE:
                seen_hashes.add(url_hash(url))  # mark as seen anyway
            continue

        formatted = gpt_format(article['title'], body)
        if not formatted:
            log.warning('  GPT failed — skip')
            continue

        entry = {
            'tag':        formatted['tag'],
            'headline':   formatted['headline'],
            'summary':    formatted['summary'],
            'url':        url,
            'scraped_at': datetime.now(timezone.utc).isoformat(),
        }

        log.info(f'  [{entry["tag"]}] {entry["headline"]}')
        log.info(f'  {entry["summary"][:90]}…')

        if not DRY_RUN:
            if USE_SUPABASE:
                if supabase_insert(entry):
                    seen_urls.add(url)
                    new_count += 1
            else:
                seen_hashes.add(url_hash(url))
                current_news.insert(0, entry)
                new_count += 1
        else:
            new_count += 1
            print(json.dumps(entry, indent=2, ensure_ascii=False))

    if not DRY_RUN:
        if USE_SUPABASE:
            if new_count:
                supabase_trim()
        else:
            save_seen_local(seen_hashes)
            if new_count:
                save_news_local(current_news)

    log.info(f'══ Done: {new_count} new article(s) ══')


if __name__ == '__main__':
    run()
