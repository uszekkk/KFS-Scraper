#!/usr/bin/env python3
"""Test scraper - skanuje urzędy z urzedy.json i zapisuje artykuły do test_articles.json."""

import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).parent
URZEDY_FILE = BASE_DIR / "urzedy.json"
OUTPUT_FILE = BASE_DIR / "test_articles.json"
ERRORS_FILE = BASE_DIR / "test_errors.json"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def fetch(url: str) -> tuple[BeautifulSoup | None, str]:
    """Zwraca (soup, error_msg). Jeśli OK to error_msg jest pusty."""
    try:
        resp = SESSION.get(url, timeout=10)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser"), ""
    except requests.RequestException as e:
        return None, str(e)


KFS_KEYWORDS = re.compile(
    r"KFS|Krajow\w+ Fundusz\w* Szkoleniow|"
    r"kszta[łl]ceni\w+ ustawiczn|"
    r"nab[oó]r wniosk[oó]w",
    re.IGNORECASE,
)


def fetch_detail_content(url: str) -> str:
    """Pobiera pełną treść ze strony szczegółowej artykułu."""
    try:
        resp = SESSION.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Usuń niepotrzebne elementy
        for tag in soup.find_all(["script", "style", "nav", "footer"]):
            tag.decompose()

        # Zbierz WSZYSTKICH kandydatów
        candidates = []
        selectors = [
            ("div", {"class": re.compile(r"journal-content-article")}),
            ("div", {"class": re.compile(r"asset-content")}),
            ("div", {"class": re.compile(r"asset-full-content")}),
            ("div", {"class": re.compile(r"portlet-body")}),
            ("article", {}),
            ("main", {}),
        ]
        for tag, attrs in selectors:
            found_list = soup.find_all(tag, attrs) if attrs else soup.find_all(tag)
            for found in found_list:
                text = found.get_text(separator="\n", strip=True)
                if len(text) > 100:
                    has_kfs = bool(KFS_KEYWORDS.search(text))
                    candidates.append((text, has_kfs))

        if not candidates:
            return ""

        # Priorytet: kandydaci z treścią KFS, potem reszta
        kfs_candidates = [t for t, k in candidates if k]
        if kfs_candidates:
            best = max(kfs_candidates, key=len)
        else:
            best = max((t for t, _ in candidates), key=len)

        lines = [l.strip() for l in best.split("\n") if l.strip()]
        text = "\n".join(lines)
        if len(text) <= 3000:
            return text
        # Smart truncation: find section with dates/amounts and include it
        date_pattern = re.compile(r"\d{2}\.\d{2}\.\d{4}")
        amount_pattern = re.compile(r"\d[\d\s.,]+\s*z[łl]", re.IGNORECASE)
        # Find last occurrence of date or amount
        date_matches = list(date_pattern.finditer(text))
        amount_matches = list(amount_pattern.finditer(text))
        last_important = 0
        if date_matches:
            last_important = max(last_important, date_matches[-1].end())
        if amount_matches:
            last_important = max(last_important, amount_matches[-1].end())
        if last_important > 3000:
            # Include start (context) + end section with dates/amounts
            tail_size = min(last_important + 200, len(text)) - max(0, last_important - 1500)
            head_size = 3000 - tail_size - 20  # 20 for separator
            if head_size < 500:
                head_size = 500
                tail_start = max(0, last_important - 1500)
                return text[:head_size] + "\n[...]\n" + text[tail_start:tail_start + 3000 - head_size - 10]
            tail_start = max(0, last_important - 1500)
            tail_end = min(len(text), last_important + 200)
            return text[:head_size] + "\n[...]\n" + text[tail_start:tail_end]
        return text[:3000]
    except Exception:
        return ""


def extract_articles(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """Wyciąga artykuły ze strony głównej Liferay CMS."""
    seen_titles = set()
    articles = []

    for h3 in soup.find_all("h3"):
        parent = h3.parent
        if not parent or parent.name != "a" or not parent.get("href"):
            link = h3.find("a", href=True)
            if link:
                href = link["href"]
                title = link.get_text(strip=True) or h3.get_text(strip=True)
            else:
                continue
        else:
            href = parent["href"]
            title = h3.get_text(strip=True)

        if not title:
            continue

        if not href.startswith("http"):
            href = urljoin(base_url, href)

        title_key = title.strip().lower()
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)

        # --- Data ---
        pub_date = ""
        slider_content = h3.find_parent(
            "div", class_=re.compile(r"nnk_slider-item-content")
        )
        if slider_content:
            date_p = slider_content.find(
                "p", class_=re.compile(r"nnk_slider-asset-publish-date")
            )
            if date_p:
                dm = re.search(r"\d{2}\.\d{2}\.\d{4}", date_p.get_text())
                if dm:
                    pub_date = dm.group()

        if not pub_date:
            container = h3.find_parent(
                "div", class_=re.compile(r"nnk|asset|results-row")
            )
            if container:
                dm = re.search(r"\d{2}\.\d{2}\.\d{4}", container.get_text())
                if dm:
                    pub_date = dm.group()

        if not pub_date:
            prev_text = h3.find_previous(string=re.compile(r"\d{2}\.\d{2}\.\d{4}"))
            if prev_text:
                dm = re.search(r"\d{2}\.\d{2}\.\d{4}", prev_text)
                if dm:
                    pub_date = dm.group()

        # --- Snippet ---
        snippet = ""
        if slider_content:
            for p in slider_content.find_all("p"):
                cls_str = " ".join(p.get("class", []))
                if "publish-date" in cls_str or "date" in cls_str:
                    continue
                txt = p.get_text(strip=True)
                if txt and len(txt) > 10:
                    snippet = txt
                    break

        if not snippet:
            anchor = parent if parent and parent.name == "a" else h3
            next_p = anchor.find_next("p")
            if next_p:
                txt = next_p.get_text(strip=True)
                if txt:
                    snippet = txt

        if len(snippet) > 3000:
            snippet = snippet[:3000]

        articles.append({
            "title": title,
            "url": href,
            "snippet": snippet,
            "date": pub_date,
            "source_type": "Aktualnosci",
        })

    return articles


def extract_kfs(soup: BeautifulSoup, kfs_url: str) -> dict | None:
    """Wyciąga główną treść ze strony KFS."""
    content_div = soup.find("div", class_=re.compile(r"journal-content-article"))
    if not content_div:
        content_div = soup.find("div", class_=re.compile(r"portlet-body"))
    if not content_div:
        content_div = soup.find("main")
    if not content_div:
        return None

    text = content_div.get_text(separator="\n", strip=True)
    if not text or len(text) < 20:
        return None

    return {
        "title": "KFS",
        "url": kfs_url,
        "snippet": text[:3000],
        "date": "",
        "source_type": "KFS",
    }


def main():
    with open(URZEDY_FILE, "r", encoding="utf-8") as f:
        urzedy = json.load(f)

    print(f"Zaladowano {len(urzedy)} urzedow z {URZEDY_FILE.name}")

    all_articles = []
    errors = []

    for i, urzad in enumerate(urzedy, 1):
        name = urzad["name"]
        homepage = urzad.get("homepage", "")
        kfs_url = urzad.get("kfs_url", "")
        base_url = urzad.get("base_url", homepage.rstrip("/"))

        print(f"\n[{i}/{len(urzedy)}] {name}")

        # --- Strona glowna (fallback na aktualnosci_url) ---
        aktualnosci_url = urzad.get("aktualnosci_url", "")
        soup, err = fetch(homepage)
        time.sleep(0.5)

        news_articles = []
        news_error = ""
        used_url = homepage
        if soup:
            news_articles = extract_articles(soup, base_url)

        # Fallback: jeśli homepage dał 0 artykułów, spróbuj aktualnosci_url
        if len(news_articles) == 0 and aktualnosci_url and aktualnosci_url != homepage:
            print(f"  Homepage: 0 artykulow, probuje aktualnosci_url...")
            soup2, err2 = fetch(aktualnosci_url)
            time.sleep(0.5)
            if soup2:
                news_articles = extract_articles(soup2, base_url)
                used_url = aktualnosci_url
            if len(news_articles) == 0:
                err = err2 if not soup2 else "Obie strony (homepage + aktualnosci) bez artykulow"

        if news_articles:
            for a in news_articles:
                a["urzad"] = name
            # Pobierz pełną treść ze stron szczegółowych
            enriched = 0
            for a in news_articles:
                detail = fetch_detail_content(a["url"])
                if detail and len(detail) > len(a.get("snippet", "")):
                    a["snippet"] = detail
                    enriched += 1
                time.sleep(0.3)
            print(f"  Aktualnosci: {len(news_articles)} (wzbogacono {enriched})")
        elif soup or used_url != homepage:
            news_error = err or "Strona pobrana OK ale nie znaleziono artykulow"
            print(f"  Aktualnosci: 0 ({news_error[:60]})")
        else:
            news_error = err
            print(f"  Aktualnosci: BLAD - {err[:80]}")

        # --- Strona KFS ---
        kfs_soup, kfs_err = fetch(kfs_url)
        time.sleep(0.5)

        kfs_article = None
        kfs_error = ""
        if kfs_soup:
            kfs_article = extract_kfs(kfs_soup, kfs_url)
            if kfs_article:
                kfs_article["urzad"] = name
                print(f"  KFS: OK")
            else:
                kfs_error = "Strona pobrana OK ale nie znaleziono tresci"
                print(f"  KFS: brak tresci")
        else:
            kfs_error = kfs_err
            print(f"  KFS: BLAD - {kfs_err[:80]}")

        # Zbierz artykuly
        all_articles.extend(news_articles)
        if kfs_article:
            all_articles.append(kfs_article)

        # Zbierz bledy
        if news_error:
            errors.append({
                "urzad": name,
                "url": homepage,
                "typ": "Aktualnosci",
                "blad": news_error,
            })
        if kfs_error:
            errors.append({
                "urzad": name,
                "url": kfs_url,
                "typ": "KFS",
                "blad": kfs_error,
            })

    # Zapisz
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)

    with open(ERRORS_FILE, "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    ok_count = len(urzedy) - len([e for e in errors if e["typ"] == "Aktualnosci"])
    print(f"\n{'='*60}")
    print(f"PODSUMOWANIE")
    print(f"{'='*60}")
    print(f"Urzedow: {len(urzedy)}")
    print(f"Artykulow: {len(all_articles)}")
    print(f"Bledow: {len(errors)} (aktualnosci: {len([e for e in errors if e['typ']=='Aktualnosci'])}, KFS: {len([e for e in errors if e['typ']=='KFS'])})")
    print(f"Zapisano: {OUTPUT_FILE.name}, {ERRORS_FILE.name}")


if __name__ == "__main__":
    main()
