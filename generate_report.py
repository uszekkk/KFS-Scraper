#!/usr/bin/env python3
"""Generuje raport HTML z wynikami scrapera."""

import json
import html
import re
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
CACHE_FILE = BASE_DIR / "cache.json"
ARTICLES_FILE = BASE_DIR / "articles.json"
URZEDY_FILE = BASE_DIR / "urzedy.json"
OUTPUT_FILE = BASE_DIR / "output.html"


def load_json(path: Path) -> dict | list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_date_for_sort(date_str: str) -> str:
    """Konwertuje DD.MM.YYYY na YYYY-MM-DD do sortowania."""
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return "0000-00-00"


def e(text: str) -> str:
    """HTML-escape."""
    return html.escape(str(text)) if text else ""


def build_article_card(art: dict, cache: dict) -> str:
    url = art.get("url", "")
    classification = cache.get(url, {})
    wynik = classification.get("wynik", "NIE")
    powod = classification.get("powod", "")

    is_tak = wynik == "TAK"
    badge_cls = "badge-tak" if is_tak else "badge-nie"
    badge_text = "Nabor" if is_tak else "Inne"

    source = art.get("source_type", "Aktualnosci")
    source_cls = "src-kfs" if source == "KFS" else "src-news"

    snippet = art.get("snippet", "")
    if len(snippet) > 200:
        snippet = snippet[:200] + "..."

    date_str = art.get("date", "")
    date_display = f'<span class="date">{e(date_str)}</span>' if date_str else ""

    sort_key = parse_date_for_sort(date_str)

    powod_html = ""
    if powod:
        powod_html = f'<div class="powod">AI: {e(powod)}</div>'

    return f'''<div class="card" data-wynik="{e(wynik)}" data-sort="{sort_key}"
     data-search="{e((art.get('urzad','') + ' ' + art.get('title','') + ' ' + snippet).lower())}">
  <div class="card-header">
    <strong class="urzad">{e(art.get('urzad', ''))}</strong>
    <span class="{badge_cls}">{badge_text}</span>
    <span class="{source_cls}">{e(source)}</span>
    {date_display}
  </div>
  <div class="card-title">
    <a href="{e(url)}" target="_blank" rel="noopener">{e(art.get('title', 'Bez tytulu'))}</a>
  </div>
  <div class="snippet">{e(snippet)}</div>
  {powod_html}
</div>'''


def main():
    cache = load_json(CACHE_FILE)
    articles = load_json(ARTICLES_FILE)
    # urzedy.json loaded for reference but articles already have urzad name

    # Sort by date descending
    articles.sort(key=lambda a: parse_date_for_sort(a.get("date", "")), reverse=True)

    # Build cards
    all_cards = []
    tak_cards = []
    for art in articles:
        card_html = build_article_card(art, cache)
        all_cards.append(card_html)
        url = art.get("url", "")
        if cache.get(url, {}).get("wynik") == "TAK":
            tak_cards.append(card_html)

    count_all = len(all_cards)
    count_tak = len(tak_cards)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    html_out = f'''<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nabory PUP - Raport</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
       background: #f5f5f5; color: #333; line-height: 1.5; }}
.header {{ background: #1a3a5c; color: #fff; padding: 20px 30px; }}
.header h1 {{ font-size: 22px; margin-bottom: 6px; }}
.header .meta {{ font-size: 14px; opacity: 0.85; }}
.tabs {{ display: flex; background: #fff; border-bottom: 2px solid #ddd; padding: 0 20px;
         position: sticky; top: 0; z-index: 10; }}
.tab {{ padding: 12px 24px; cursor: pointer; border-bottom: 3px solid transparent;
        font-size: 15px; font-weight: 500; color: #666; user-select: none; }}
.tab:hover {{ color: #333; background: #f9f9f9; }}
.tab.active {{ color: #1a3a5c; border-bottom-color: #1a3a5c; }}
.tab-content {{ display: none; padding: 20px; max-width: 960px; margin: 0 auto; }}
.tab-content.active {{ display: block; }}
.search-box {{ width: 100%; padding: 10px 14px; font-size: 15px; border: 1px solid #ccc;
               border-radius: 6px; margin-bottom: 16px; }}
.search-box:focus {{ outline: none; border-color: #1a3a5c; }}
.card {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 8px;
         padding: 14px 18px; margin-bottom: 10px; }}
.card.hidden {{ display: none; }}
.card-header {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
                margin-bottom: 6px; font-size: 14px; }}
.urzad {{ font-size: 14px; }}
.badge-tak {{ background: #16a34a; color: #fff; padding: 2px 8px; border-radius: 4px;
              font-size: 12px; font-weight: 600; }}
.badge-nie {{ background: #9ca3af; color: #fff; padding: 2px 8px; border-radius: 4px;
              font-size: 12px; font-weight: 600; }}
.src-news {{ background: #e0e7ff; color: #3730a3; padding: 2px 8px; border-radius: 4px;
             font-size: 11px; }}
.src-kfs {{ background: #fef3c7; color: #92400e; padding: 2px 8px; border-radius: 4px;
            font-size: 11px; }}
.date {{ color: #888; font-size: 13px; }}
.card-title {{ margin-bottom: 4px; }}
.card-title a {{ color: #1a3a5c; text-decoration: none; font-size: 15px; font-weight: 500; }}
.card-title a:hover {{ text-decoration: underline; }}
.snippet {{ font-size: 13px; color: #555; margin-bottom: 4px; }}
.powod {{ font-size: 12px; color: #999; font-style: italic; }}
.no-results {{ text-align: center; padding: 40px; color: #999; display: none; }}
</style>
</head>
<body>

<div class="header">
  <h1>Nabory wniosków - Powiatowe Urzędy Pracy</h1>
  <div class="meta">Wygenerowano: {now} | Nabory: {count_tak} | Wszystkie artykuły: {count_all}</div>
</div>

<div class="tabs">
  <div class="tab active" data-tab="nabory">🟢 Nabory wniosków ({count_tak})</div>
  <div class="tab" data-tab="wszystkie">📋 Wszystkie aktualności ({count_all})</div>
</div>

<div class="tab-content active" id="tab-nabory">
  <input type="text" class="search-box" placeholder="Szukaj w naborach..." data-target="nabory">
  <div class="card-list" id="list-nabory">
    {"".join(tak_cards)}
  </div>
  <div class="no-results" id="no-nabory">Brak wyników dla podanego zapytania.</div>
</div>

<div class="tab-content" id="tab-wszystkie">
  <input type="text" class="search-box" placeholder="Szukaj we wszystkich..." data-target="wszystkie">
  <div class="card-list" id="list-wszystkie">
    {"".join(all_cards)}
  </div>
  <div class="no-results" id="no-wszystkie">Brak wyników dla podanego zapytania.</div>
</div>

<script>
// Tabs
document.querySelectorAll('.tab').forEach(function(tab) {{
  tab.addEventListener('click', function() {{
    document.querySelectorAll('.tab').forEach(function(t) {{ t.classList.remove('active'); }});
    document.querySelectorAll('.tab-content').forEach(function(c) {{ c.classList.remove('active'); }});
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
  }});
}});

// Search
document.querySelectorAll('.search-box').forEach(function(input) {{
  input.addEventListener('input', function() {{
    var query = input.value.toLowerCase();
    var target = input.dataset.target;
    var list = document.getElementById('list-' + target);
    var cards = list.querySelectorAll('.card');
    var noResults = document.getElementById('no-' + target);
    var visible = 0;
    cards.forEach(function(card) {{
      var match = card.dataset.search.indexOf(query) !== -1;
      card.classList.toggle('hidden', !match);
      if (match) visible++;
    }});
    noResults.style.display = visible === 0 ? 'block' : 'none';
  }});
}});
</script>

</body>
</html>'''

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html_out)

    print(f"Raport zapisany do {OUTPUT_FILE}")
    print(f"  Nabory: {count_tak}")
    print(f"  Wszystkie: {count_all}")


if __name__ == "__main__":
    main()
