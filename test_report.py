#!/usr/bin/env python3
"""Generuje test_output.html z wynikami klasyfikacji + zakładka błędów + mapa Polski z powiatami."""

import json
import html
import re
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
RESULTS_FILE = BASE_DIR / "test_results.json"
ERRORS_FILE = BASE_DIR / "test_errors.json"
GEOJSON_FILE = BASE_DIR / "powiaty.geojson"
MAPPING_FILE = BASE_DIR / "urzad_to_powiat.json"
WOJ_FILE = BASE_DIR / "urzad_to_woj.json"
OUTPUT_FILE = BASE_DIR / "test_output.html"


def e(text):
    return html.escape(str(text)) if text else ""


def date_sort_key(date_str):
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_str or "")
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else "0000-00-00"


def parse_termin_dates(termin_str):
    """Parsuje termin '30.03.2026 - 03.04.2026' na (from_iso, to_iso)."""
    if not termin_str:
        return "", ""
    dates = re.findall(r"(\d{2})\.(\d{2})\.(\d{4})", termin_str)
    if len(dates) >= 2:
        return (f"{dates[0][2]}-{dates[0][1]}-{dates[0][0]}",
                f"{dates[1][2]}-{dates[1][1]}-{dates[1][0]}")
    elif len(dates) == 1:
        iso = f"{dates[0][2]}-{dates[0][1]}-{dates[0][0]}"
        return iso, iso
    return "", ""


def card(r, urzad_to_woj):
    is_tak = r["wynik"] == "TAK"
    badge = '<span class="badge tak">NABOR</span>' if is_tak else '<span class="badge nie">INNE</span>'
    date = f'<span class="date">{e(r["date"])}</span>' if r.get("date") else ""
    termin = r.get("termin", "")
    kwota = r.get("kwota", "")
    termin_html = f'<span class="termin">{e(termin)}</span>' if termin and is_tak else ""
    kwota_html = f'<span class="kwota">{e(kwota)}</span>' if kwota and is_tak else ""
    snippet = r.get("snippet", "")
    if len(snippet) > 200:
        snippet = snippet[:200] + "..."
    powod = r.get("powod", "")
    search_data = f'{r.get("urzad","")} {r.get("title","")} {snippet}'.lower()

    woj = urzad_to_woj.get(r.get("urzad", ""), "")
    date_sort = date_sort_key(r.get("date", ""))
    wynik = r.get("wynik", "")
    termin_from, termin_to = parse_termin_dates(termin)

    return f'''<div class="card" data-search="{e(search_data)}" data-woj="{e(woj)}" data-date="{date_sort}" data-wynik="{e(wynik)}" data-termin-from="{termin_from}" data-termin-to="{termin_to}">
  <div class="row1">
    <span class="urzad">{e(r.get("urzad",""))}</span>
    {badge}
    {termin_html}
    {kwota_html}
    {date}
  </div>
  <div class="title"><a href="{e(r.get("url",""))}" target="_blank" rel="noopener">{e(r.get("title",""))}</a></div>
  <div class="snippet">{e(snippet)}</div>
  <div class="ai">AI: {e(powod)}</div>
</div>'''


def error_card(err):
    search_data = f'{err.get("urzad","")} {err.get("typ","")} {err.get("blad","")}'.lower()
    typ_cls = "err-kfs" if err.get("typ") == "KFS" else "err-news"
    return f'''<div class="card err-card" data-search="{e(search_data)}">
  <div class="row1">
    <span class="urzad">{e(err.get("urzad",""))}</span>
    <span class="{typ_cls}">{e(err.get("typ",""))}</span>
  </div>
  <div class="err-url"><a href="{e(err.get("url",""))}" target="_blank" rel="noopener">{e(err.get("url",""))}</a></div>
  <div class="err-msg">{e(err.get("blad",""))}</div>
</div>'''


def build_geojson_with_status(results, geojson, urzad_to_powiat):
    """Wzbogaca GeoJSON powiatów o dane o naborach KFS."""
    # Grupuj wyniki per urząd
    urzad_data = {}
    for r in results:
        urzad = r.get("urzad", "")
        if not urzad:
            continue
        if urzad not in urzad_data:
            urzad_data[urzad] = {"tak": [], "total": 0}
        urzad_data[urzad]["total"] += 1
        if r["wynik"] == "TAK":
            urzad_data[urzad]["tak"].append({
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "date": r.get("date", ""),
                "powod": r.get("powod", ""),
                "termin": r.get("termin", ""),
                "kwota": r.get("kwota", ""),
            })

    # Mapuj nazwy powiatów GeoJSON -> status
    powiat_to_status = {}  # nazwa_geojson -> {urzad, tak, articles}
    for urzad, geo_name in urzad_to_powiat.items():
        if urzad in urzad_data:
            data = urzad_data[urzad]
            existing = powiat_to_status.get(geo_name)
            if existing:
                # Merge (np. Kielce MUP + Kielce PUP -> ten sam powiat)
                existing["tak"] = existing["tak"] or len(data["tak"]) > 0
                existing["count"] += len(data["tak"])
                existing["articles"].extend(data["tak"][:3])
                existing["urzedy"].append(urzad)
            else:
                powiat_to_status[geo_name] = {
                    "urzedy": [urzad],
                    "tak": len(data["tak"]) > 0,
                    "count": len(data["tak"]),
                    "articles": data["tak"][:5],
                }

    # Wzbogać każdy feature w GeoJSON
    for feat in geojson["features"]:
        nazwa = feat["properties"]["nazwa"]
        status = powiat_to_status.get(nazwa)
        if status:
            feat["properties"]["has_tak"] = status["tak"]
            feat["properties"]["tak_count"] = status["count"]
            feat["properties"]["urzedy"] = ", ".join(status["urzedy"])
            feat["properties"]["articles"] = json.dumps(status["articles"][:5], ensure_ascii=False)
        else:
            feat["properties"]["has_tak"] = False
            feat["properties"]["tak_count"] = 0
            feat["properties"]["urzedy"] = ""
            feat["properties"]["articles"] = "[]"

    return geojson


def main():
    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        results = json.load(f)

    errors = []
    if ERRORS_FILE.exists():
        with open(ERRORS_FILE, "r", encoding="utf-8") as f:
            errors = json.load(f)

    geojson = {"type": "FeatureCollection", "features": []}
    if GEOJSON_FILE.exists():
        with open(GEOJSON_FILE, "r", encoding="utf-8") as f:
            geojson = json.load(f)

    urzad_to_powiat = {}
    if MAPPING_FILE.exists():
        with open(MAPPING_FILE, "r", encoding="utf-8") as f:
            urzad_to_powiat = json.load(f)

    urzad_to_woj = {}
    if WOJ_FILE.exists():
        with open(WOJ_FILE, "r", encoding="utf-8") as f:
            urzad_to_woj = json.load(f)

    tak = [r for r in results if r["wynik"] == "TAK"]
    nie = [r for r in results if r["wynik"] == "NIE"]
    total = len(results)
    count_tak = len(tak)
    count_err = len(errors)

    tak_powiaty = sorted(set(r["urzad"] for r in tak))
    count_tak_powiaty = len(tak_powiaty)

    tak.sort(key=lambda r: date_sort_key(r.get("date", "")), reverse=True)
    nie.sort(key=lambda r: date_sort_key(r.get("date", "")), reverse=True)
    all_sorted = tak + nie

    tak_cards = "\n".join(card(r, urzad_to_woj) for r in tak)
    all_cards = "\n".join(card(r, urzad_to_woj) for r in all_sorted)
    err_cards = "\n".join(error_card(err) for err in errors)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Build województwo options for filters
    woj_set = sorted(set(urzad_to_woj.values()))
    woj_options = "\n".join(f'<option value="{e(w)}">{e(w)}</option>' for w in woj_set)

    # Build enriched GeoJSON for map
    enriched_geojson = build_geojson_with_status(results, geojson, urzad_to_powiat)
    geojson_json = json.dumps(enriched_geojson, ensure_ascii=False)

    # Zlicz typy błędów
    err_news = len([x for x in errors if x.get("typ") == "Aktualnosci"])
    err_kfs = len([x for x in errors if x.get("typ") == "KFS"])
    err_zero = len([x for x in errors if "nie znaleziono" in x.get("blad", "").lower()])
    err_http = count_err - err_zero

    page = f'''<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Nabory KFS - Raport</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
       background:#f0f2f5; color:#1a1a1a; }}

.header {{ background:linear-gradient(135deg,#1e3a5f,#2d5a8e); color:#fff;
           padding:28px 32px; }}
.header h1 {{ font-size:22px; font-weight:600; margin-bottom:4px; }}
.header .meta {{ font-size:14px; opacity:.85; }}
.header .meta2 {{ font-size:13px; opacity:.7; margin-top:2px; }}

.tabs {{ display:flex; background:#fff; border-bottom:2px solid #e5e7eb;
         position:sticky; top:0; z-index:1000; flex-wrap:wrap; }}
.tab {{ padding:14px 24px; cursor:pointer; font-size:15px; font-weight:500;
        color:#666; border-bottom:3px solid transparent; user-select:none;
        transition: color .15s, border-color .15s; }}
.tab:hover {{ color:#1e3a5f; background:#f8f9fa; }}
.tab.active {{ color:#1e3a5f; border-bottom-color:#1e3a5f; }}
.tab.err-tab {{ color:#b91c1c; }}
.tab.err-tab.active {{ color:#b91c1c; border-bottom-color:#b91c1c; }}
.tab.map-tab {{ color:#059669; }}
.tab.map-tab.active {{ color:#059669; border-bottom-color:#059669; }}

.panel {{ display:none; max-width:900px; margin:0 auto; padding:20px; }}
.panel.active {{ display:block; }}
.panel.map-panel {{ max-width:100%; padding:0; }}

.search {{ width:100%; padding:10px 16px; font-size:15px; border:1px solid #d1d5db;
           border-radius:8px; margin-bottom:12px; outline:none; }}
.search:focus {{ border-color:#2d5a8e; box-shadow:0 0 0 3px rgba(45,90,142,.15); }}

.filters {{ background:#fff; border:1px solid #e5e7eb; border-radius:10px;
            padding:16px 20px; margin-bottom:16px; }}
.filters-header {{ display:flex; align-items:center; justify-content:space-between;
                   cursor:pointer; user-select:none; }}
.filters-header h3 {{ font-size:14px; color:#1e3a5f; margin:0; }}
.filters-header .toggle {{ font-size:12px; color:#9ca3af; }}
.filters-body {{ display:none; margin-top:14px; }}
.filters-body.open {{ display:block; }}
.filters-row {{ display:flex; gap:14px; flex-wrap:wrap; align-items:flex-end; }}
.filter-group {{ display:flex; flex-direction:column; gap:4px; min-width:160px; flex:1; }}
.filter-group label {{ font-size:12px; font-weight:600; color:#6b7280; text-transform:uppercase;
                       letter-spacing:.3px; }}
.filter-group select,
.filter-group input[type="date"] {{ padding:8px 10px; font-size:14px; border:1px solid #d1d5db;
                                    border-radius:6px; outline:none; background:#fff; }}
.filter-group select:focus,
.filter-group input[type="date"]:focus {{ border-color:#2d5a8e; box-shadow:0 0 0 2px rgba(45,90,142,.12); }}
.filter-actions {{ display:flex; gap:8px; align-items:flex-end; }}
.btn-clear {{ padding:8px 16px; font-size:13px; border:1px solid #d1d5db; border-radius:6px;
              background:#fff; color:#6b7280; cursor:pointer; white-space:nowrap; }}
.btn-clear:hover {{ background:#f3f4f6; color:#1a1a1a; }}
.filter-count {{ font-size:13px; color:#6b7280; margin-bottom:8px; }}
.filter-count b {{ color:#1e3a5f; }}

.card {{ background:#fff; border:1px solid #e5e7eb; border-radius:10px;
         padding:16px 20px; margin-bottom:10px;
         transition: box-shadow .15s; }}
.card:hover {{ box-shadow:0 2px 8px rgba(0,0,0,.08); }}
.card.hidden {{ display:none; }}

.err-card {{ border-left:4px solid #ef4444; }}

.row1 {{ display:flex; align-items:center; gap:10px; flex-wrap:wrap; margin-bottom:6px; }}
.urzad {{ font-weight:700; font-size:15px; }}
.badge {{ padding:2px 10px; border-radius:5px; font-size:12px; font-weight:700;
          text-transform:uppercase; letter-spacing:.3px; }}
.badge.tak {{ background:#dcfce7; color:#166534; }}
.badge.nie {{ background:#f3f4f6; color:#6b7280; }}
.termin {{ background:#dbeafe; color:#1e40af; padding:2px 8px; border-radius:5px;
           font-size:12px; font-weight:600; white-space:nowrap; }}
.kwota {{ background:#fef3c7; color:#92400e; padding:2px 8px; border-radius:5px;
          font-size:12px; font-weight:600; white-space:nowrap; }}
.date {{ margin-left:auto; color:#9ca3af; font-size:13px; }}

.err-news {{ background:#fef3c7; color:#92400e; padding:2px 10px; border-radius:5px;
             font-size:12px; font-weight:600; }}
.err-kfs {{ background:#e0e7ff; color:#3730a3; padding:2px 10px; border-radius:5px;
            font-size:12px; font-weight:600; }}

.title {{ margin-bottom:4px; }}
.title a {{ color:#1e3a5f; text-decoration:none; font-size:15px; font-weight:500; }}
.title a:hover {{ text-decoration:underline; }}

.snippet {{ font-size:13px; color:#6b7280; line-height:1.5; margin-bottom:4px; }}
.ai {{ font-size:12px; color:#9ca3af; font-style:italic; }}

.err-url {{ font-size:13px; margin-bottom:4px; }}
.err-url a {{ color:#1e3a5f; text-decoration:none; word-break:break-all; }}
.err-url a:hover {{ text-decoration:underline; }}
.err-msg {{ font-size:13px; color:#b91c1c; }}

.err-summary {{ background:#fef2f2; border:1px solid #fecaca; border-radius:8px;
                padding:14px 18px; margin-bottom:16px; font-size:14px; color:#991b1b; }}

.empty {{ text-align:center; padding:48px; color:#9ca3af; font-size:15px; display:none; }}

/* Map styles */
#map {{ width:100%; height:calc(100vh - 140px); min-height:500px; }}

.map-legend {{
    background:rgba(255,255,255,.95); padding:14px 18px; border-radius:10px;
    box-shadow:0 2px 12px rgba(0,0,0,.15); font-size:13px; line-height:2;
    backdrop-filter:blur(4px);
}}
.map-legend h4 {{ margin:0 0 8px 0; font-size:14px; color:#1e3a5f; }}
.map-legend .legend-item {{ display:flex; align-items:center; gap:8px; }}
.map-legend .swatch {{ width:20px; height:14px; border-radius:3px; display:inline-block;
                       border:1px solid rgba(0,0,0,.2); }}

.map-stats {{
    position:absolute; top:10px; right:10px; z-index:999;
    background:rgba(255,255,255,.95); padding:14px 18px; border-radius:10px;
    box-shadow:0 2px 12px rgba(0,0,0,.15); font-size:13px;
    backdrop-filter:blur(4px);
}}
.map-stats h4 {{ margin:0 0 6px 0; font-size:14px; color:#1e3a5f; }}
.map-stats .stat {{ margin:2px 0; }}
.map-stats .stat-num {{ font-weight:700; color:#166534; }}

.leaflet-popup-content {{ max-width:320px; }}
.popup-title {{ font-weight:700; font-size:14px; color:#1e3a5f; margin-bottom:8px;
                border-bottom:2px solid #22c55e; padding-bottom:6px; }}
.popup-title.no-tak {{ border-bottom-color:#d1d5db; color:#6b7280; }}
.popup-article {{ margin:6px 0; padding:4px 0; border-bottom:1px solid #f0f0f0; }}
.popup-article:last-child {{ border-bottom:none; }}
.popup-article a {{ color:#1e3a5f; text-decoration:none; font-size:12px; font-weight:500; }}
.popup-article a:hover {{ text-decoration:underline; }}
.popup-article .popup-date {{ font-size:11px; color:#9ca3af; }}
.popup-article .popup-termin {{ font-size:11px; color:#1e40af; font-weight:600; }}
.popup-article .popup-powod {{ font-size:11px; color:#6b7280; font-style:italic; }}
.popup-no {{ font-size:12px; color:#9ca3af; }}

.info-hover {{
    background:rgba(255,255,255,.92); padding:8px 12px; border-radius:8px;
    box-shadow:0 2px 8px rgba(0,0,0,.12); font-size:13px;
    backdrop-filter:blur(4px);
}}
.info-hover b {{ color:#1e3a5f; }}
.info-hover .info-tak {{ color:#166534; font-weight:700; }}
.info-hover .info-nie {{ color:#9ca3af; }}
</style>
</head>
<body>

<div class="header">
  <h1>Nabory KFS - Powiatowe Urzedy Pracy</h1>
  <div class="meta">Wygenerowano: {now} &nbsp;|&nbsp; Znaleziono {count_tak} naborow KFS w {count_tak_powiaty} powiatach / {total} artykulow lacznie</div>
  <div class="meta2">Bledow scrapera: {count_err} (HTTP: {err_http}, brak artykulow: {err_zero})</div>
</div>

<div class="tabs">
  <div class="tab map-tab active" data-tab="map">&#x1F5FA; Mapa ({count_tak_powiaty})</div>
  <div class="tab" data-tab="nabory">&#x1F7E2; Nabory KFS ({count_tak})</div>
  <div class="tab" data-tab="all">&#x1F4CB; Wszystkie ({total})</div>
  <div class="tab err-tab" data-tab="errors">&#x26A0; Bledy ({count_err})</div>
</div>

<div class="panel map-panel active" id="p-map">
  <div style="position:relative;">
    <div id="map"></div>
    <div class="map-stats">
      <h4>Statystyki</h4>
      <div class="stat">Nabory KFS: <span class="stat-num">{count_tak}</span></div>
      <div class="stat">Powiaty z naborem: <span class="stat-num">{count_tak_powiaty}</span></div>
      <div class="stat">Wszystkich artykulow: {total}</div>
    </div>
  </div>
</div>

<div class="panel" id="p-nabory">
  <input class="search" placeholder="Szukaj po urzedzie lub tytule..." data-list="l-nabory">
  <div class="filters" id="f-nabory">
    <div class="filters-header" onclick="toggleFilters('f-nabory')">
      <h3>Filtry</h3>
      <span class="toggle" id="f-nabory-toggle">Rozwin</span>
    </div>
    <div class="filters-body" id="f-nabory-body">
      <div class="filters-row">
        <div class="filter-group">
          <label>Wojewodztwo</label>
          <select data-filter="woj" data-list="l-nabory">
            <option value="">-- Wszystkie --</option>
            {woj_options}
          </select>
        </div>
        <div class="filter-group">
          <label>Termin naboru od</label>
          <input type="date" data-filter="termin-from" data-list="l-nabory">
        </div>
        <div class="filter-group">
          <label>Termin naboru do</label>
          <input type="date" data-filter="termin-to" data-list="l-nabory">
        </div>
        <div class="filter-group">
          <label>Data publikacji od</label>
          <input type="date" data-filter="date-from" data-list="l-nabory">
        </div>
        <div class="filter-group">
          <label>Data publikacji do</label>
          <input type="date" data-filter="date-to" data-list="l-nabory">
        </div>
        <div class="filter-actions">
          <button class="btn-clear" onclick="clearFilters('nabory')">Wyczysc filtry</button>
        </div>
      </div>
    </div>
  </div>
  <div class="filter-count" id="c-nabory">Wyniki: <b>{count_tak}</b></div>
  <div id="l-nabory">{tak_cards}</div>
  <div class="empty" id="e-nabory">Brak wynikow dla wybranych filtrow.</div>
</div>

<div class="panel" id="p-all">
  <input class="search" placeholder="Szukaj po urzedzie lub tytule..." data-list="l-all">
  <div class="filters" id="f-all">
    <div class="filters-header" onclick="toggleFilters('f-all')">
      <h3>Filtry</h3>
      <span class="toggle" id="f-all-toggle">Rozwin</span>
    </div>
    <div class="filters-body" id="f-all-body">
      <div class="filters-row">
        <div class="filter-group">
          <label>Wojewodztwo</label>
          <select data-filter="woj" data-list="l-all">
            <option value="">-- Wszystkie --</option>
            {woj_options}
          </select>
        </div>
        <div class="filter-group">
          <label>Wynik AI</label>
          <select data-filter="wynik" data-list="l-all">
            <option value="">-- Wszystkie --</option>
            <option value="TAK">TAK (nabor KFS)</option>
            <option value="NIE">NIE</option>
          </select>
        </div>
        <div class="filter-group">
          <label>Termin naboru od</label>
          <input type="date" data-filter="termin-from" data-list="l-all">
        </div>
        <div class="filter-group">
          <label>Termin naboru do</label>
          <input type="date" data-filter="termin-to" data-list="l-all">
        </div>
        <div class="filter-group">
          <label>Data publikacji od</label>
          <input type="date" data-filter="date-from" data-list="l-all">
        </div>
        <div class="filter-group">
          <label>Data publikacji do</label>
          <input type="date" data-filter="date-to" data-list="l-all">
        </div>
        <div class="filter-actions">
          <button class="btn-clear" onclick="clearFilters('all')">Wyczysc filtry</button>
        </div>
      </div>
    </div>
  </div>
  <div class="filter-count" id="c-all">Wyniki: <b>{total}</b></div>
  <div id="l-all">{all_cards}</div>
  <div class="empty" id="e-all">Brak wynikow dla wybranych filtrow.</div>
</div>

<div class="panel" id="p-errors">
  <div class="err-summary">
    Lacznie {count_err} bledow: {err_news} w aktualnosciach, {err_kfs} w KFS.
    &nbsp;|&nbsp; Bledy HTTP/timeout: {err_http} &nbsp;|&nbsp; Strona OK ale 0 artykulow: {err_zero}
  </div>
  <input class="search" placeholder="Szukaj po nazwie urzedu..." data-list="l-errors">
  <div id="l-errors">{err_cards}</div>
  <div class="empty" id="e-errors">Brak bledow.</div>
</div>

<script>
// Tab switching
document.querySelectorAll(".tab").forEach(function(t){{
  t.addEventListener("click",function(){{
    document.querySelectorAll(".tab").forEach(function(x){{x.classList.remove("active")}});
    document.querySelectorAll(".panel").forEach(function(x){{x.classList.remove("active")}});
    t.classList.add("active");
    document.getElementById("p-"+t.dataset.tab).classList.add("active");
    if(t.dataset.tab==="map" && window._map) window._map.invalidateSize();
  }});
}});

// Filtering system
function toggleFilters(id){{
  var body=document.getElementById(id+"-body");
  var toggle=document.getElementById(id+"-toggle");
  var open=body.classList.toggle("open");
  toggle.textContent=open?"Zwin":"Rozwin";
}}

function getFilters(listId){{
  var panel=document.getElementById("p-"+listId.split("-")[1]);
  var searchInput=panel.querySelector(".search");
  var q=searchInput?searchInput.value.toLowerCase():"";
  var woj="",wynik="",dateFrom="",dateTo="",terminFrom="",terminTo="";
  panel.querySelectorAll("[data-filter]").forEach(function(el){{
    var f=el.dataset.filter;
    if(f==="woj") woj=el.value;
    else if(f==="wynik") wynik=el.value;
    else if(f==="date-from") dateFrom=el.value;
    else if(f==="date-to") dateTo=el.value;
    else if(f==="termin-from") terminFrom=el.value;
    else if(f==="termin-to") terminTo=el.value;
  }});
  return {{q:q,woj:woj,wynik:wynik,dateFrom:dateFrom,dateTo:dateTo,terminFrom:terminFrom,terminTo:terminTo}};
}}

function applyFilters(listId){{
  var f=getFilters(listId);
  var list=document.getElementById(listId);
  var cards=list.querySelectorAll(".card");
  var id=listId.split("-")[1];
  var empty=document.getElementById("e-"+id);
  var counter=document.getElementById("c-"+id);
  var n=0;
  cards.forEach(function(c){{
    var ok=true;
    if(f.q && c.dataset.search.indexOf(f.q)===-1) ok=false;
    if(ok && f.woj && c.dataset.woj!==f.woj) ok=false;
    if(ok && f.wynik && c.dataset.wynik!==f.wynik) ok=false;
    if(ok && f.dateFrom && c.dataset.date<f.dateFrom) ok=false;
    if(ok && f.dateTo && c.dataset.date>f.dateTo) ok=false;
    // Termin naboru: pokaż nabory aktywne w wybranym okresie
    if(ok && f.terminFrom){{
      // Nabor musi sie konczyc >= terminFrom (jeszcze trwa lub sie nie skonczyl)
      if(c.dataset.terminTo && c.dataset.terminTo<f.terminFrom) ok=false;
      if(!c.dataset.terminTo && !c.dataset.terminFrom) ok=false;
    }}
    if(ok && f.terminTo){{
      // Nabor musi sie zaczynac <= terminTo (juz sie zaczal lub zacznie sie przed)
      if(c.dataset.terminFrom && c.dataset.terminFrom>f.terminTo) ok=false;
      if(!c.dataset.terminFrom && !c.dataset.terminTo) ok=false;
    }}
    c.classList.toggle("hidden",!ok);
    if(ok)n++;
  }});
  if(empty) empty.style.display=n===0?"block":"none";
  if(counter) counter.innerHTML="Wyniki: <b>"+n+"</b>";
}}

function clearFilters(tabId){{
  var panel=document.getElementById("p-"+tabId);
  panel.querySelectorAll("[data-filter]").forEach(function(el){{
    el.value="";
  }});
  var search=panel.querySelector(".search");
  if(search) search.value="";
  applyFilters("l-"+tabId);
}}

// Bind search inputs
document.querySelectorAll(".search").forEach(function(input){{
  input.addEventListener("input",function(){{
    applyFilters(input.dataset.list);
  }});
}});

// Bind filter controls
document.querySelectorAll("[data-filter]").forEach(function(el){{
  el.addEventListener("change",function(){{
    applyFilters(el.dataset.list);
  }});
}});

// ====== MAP ======
var geojsonData = {geojson_json};

var map = L.map("map",{{
    zoomControl:true,
    scrollWheelZoom:true,
}}).setView([52.0, 19.4], 6);
window._map = map;

L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_nolabels/{{z}}/{{x}}/{{y}}{{r}}.png",{{
    attribution:'&copy; <a href="https://www.openstreetmap.org/">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    subdomains:"abcd",
    maxZoom:19,
}}).addTo(map);

// Hover info panel
var info = L.control({{position:"topright"}});
info.onAdd = function(){{
    this._div = L.DomUtil.create("div","info-hover");
    this._div.style.display = "none";
    return this._div;
}};
info.update = function(props){{
    if(!props){{ this._div.style.display="none"; return; }}
    this._div.style.display="block";
    var n = props.nazwa.replace("powiat ","").replace("Powiat ","");
    n = n.charAt(0).toUpperCase() + n.slice(1);
    if(props.has_tak){{
        this._div.innerHTML = '<b>' + n + '</b><br>'
            + '<span class="info-tak">Nabor KFS: ' + props.tak_count + '</span>'
            + (props.urzedy ? '<br><small>' + props.urzedy + '</small>' : '');
    }} else {{
        this._div.innerHTML = '<b>' + n + '</b><br><span class="info-nie">Brak naboru</span>';
    }}
}};
info.addTo(map);

// Style function
function style(feature) {{
    var hasTak = feature.properties.has_tak;
    var count = feature.properties.tak_count || 0;

    if (hasTak) {{
        var green = count >= 3 ? "#15803d" : count >= 2 ? "#22c55e" : "#4ade80";
        return {{
            fillColor: green,
            weight: 1.5,
            color: "#fff",
            fillOpacity: 0.75,
        }};
    }}
    return {{
        fillColor: "#e8e8e8",
        weight: 0.8,
        color: "#ccc",
        fillOpacity: 0.5,
    }};
}}

function highlightStyle(feature) {{
    var hasTak = feature.properties.has_tak;
    return {{
        weight: 2.5,
        color: hasTak ? "#166534" : "#666",
        fillOpacity: hasTak ? 0.9 : 0.7,
    }};
}}

var geojsonLayer;

function onEachFeature(feature, layer) {{
    layer.on({{
        mouseover: function(e) {{
            var l = e.target;
            l.setStyle(highlightStyle(feature));
            l.bringToFront();
            info.update(feature.properties);
        }},
        mouseout: function(e) {{
            geojsonLayer.resetStyle(e.target);
            info.update();
        }},
        click: function(e) {{
            var props = feature.properties;
            var html = "";
            var n = props.nazwa.replace("powiat ","").replace("Powiat ","");
            n = n.charAt(0).toUpperCase() + n.slice(1);

            if(props.has_tak) {{
                html += '<div class="popup-title">' + n;
                if(props.urzedy) html += ' <small style="font-weight:400;color:#6b7280;">(' + props.urzedy + ')</small>';
                html += '</div>';
                var articles = JSON.parse(props.articles || "[]");
                articles.forEach(function(a){{
                    html += '<div class="popup-article">';
                    html += '<a href="' + a.url + '" target="_blank">'
                         + a.title.substring(0,70) + (a.title.length>70?"...":"") + '</a>';
                    var meta = [];
                    if(a.termin) meta.push('<b>' + a.termin + '</b>');
                    if(a.kwota) meta.push(a.kwota);
                    if(meta.length) html += '<br><span class="popup-termin">' + meta.join(' | ') + '</span>';
                    else if(a.date) html += '<br><span class="popup-date">' + a.date + '</span>';
                    html += '</div>';
                }});
            }} else {{
                html += '<div class="popup-title no-tak">' + n + '</div>';
                html += '<div class="popup-no">Brak aktywnych naborow KFS</div>';
            }}
            L.popup({{maxWidth:350}}).setLatLng(e.latlng).setContent(html).openOn(map);
        }}
    }});
}}

geojsonLayer = L.geoJSON(geojsonData, {{
    style: style,
    onEachFeature: onEachFeature,
}}).addTo(map);

// Labels layer (on top)
L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_only_labels/{{z}}/{{x}}/{{y}}{{r}}.png",{{
    subdomains:"abcd",
    maxZoom:19,
    pane:"overlayPane",
}}).addTo(map);

// Legend
var legend = L.control({{position:"bottomleft"}});
legend.onAdd = function(){{
    var div = L.DomUtil.create("div","map-legend");
    div.innerHTML = '<h4>Nabory KFS</h4>'
        + '<div class="legend-item"><span class="swatch" style="background:#4ade80;"></span> 1 nabor</div>'
        + '<div class="legend-item"><span class="swatch" style="background:#22c55e;"></span> 2 nabory</div>'
        + '<div class="legend-item"><span class="swatch" style="background:#15803d;"></span> 3+ nabory</div>'
        + '<div class="legend-item"><span class="swatch" style="background:#e8e8e8;"></span> Brak naboru</div>';
    return div;
}};
legend.addTo(map);

// Fit to Poland
map.fitBounds(geojsonLayer.getBounds().pad(0.02));
</script>

</body>
</html>'''

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(page)

    print(f"Raport zapisany: {OUTPUT_FILE}")
    print(f"  Nabory KFS: {count_tak} (w {count_tak_powiaty} powiatach)")
    print(f"  Wszystkie: {total}")
    print(f"  Bledy: {count_err}")


if __name__ == "__main__":
    main()
