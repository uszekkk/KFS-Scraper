# KFS Scraper

Automatyczny scraper ogłoszeń o naborach wniosków z **Krajowego Funduszu Szkoleniowego (KFS)** z ~340 powiatowych urzędów pracy w Polsce.

Wyniki prezentowane jako interaktywna mapa Polski z listą aktywnych naborów.

## Demo

> **[uszekkk.github.io/KFS-Scraper](https://uszekkk.github.io/KFS-Scraper/)**

Strona aktualizuje się automatycznie co 5 godzin.

## Jak to działa

```
urzedy.json (lista ~340 urzędów)
      │
      ▼
  Scraping stron aktualności + podstron KFS
      │
      ▼
  Klasyfikacja AI (Gemini 2.0 Flash)
  → TAK/NIE + termin + kwota
      │
      ▼
  index.html — interaktywna mapa + lista naborów
```

1. **Scraping** — pobiera strony aktualności i podstrony KFS z powiatowych urzędów pracy (Liferay CMS)
2. **Klasyfikacja** — Gemini 2.0 Flash ocenia każdy artykuł: czy to aktualny nabór KFS? Wyciąga termin składania wniosków i kwotę środków
3. **Raport** — generuje samodzielny plik HTML z mapą Leaflet, kolorując powiaty z aktywnymi naborami

## Funkcje

- Interaktywna mapa Polski z podziałem na powiaty
- Powiaty z naborami podświetlone na zielono
- Wyszukiwarka po nazwie urzędu
- Szczegóły naboru: termin, kwota, link do ogłoszenia
- Cache klasyfikacji (nie powtarza zapytań do API)
- Filtrowanie starych artykułów (< 2026)

## Automatyzacja

GitHub Actions uruchamia pipeline co 5 godzin (`cron: '0 */5 * * *'`). Wyniki commitowane do repo, strona deployowana na GitHub Pages.

Można też uruchomić ręcznie: Actions → KFS Scraper → Run workflow.

## Uruchomienie lokalne

```bash
pip install requests beautifulsoup4
export GEMINI_API_KEYS='klucz1,klucz2,...'
python run.py
```

Wymaga kluczy API [Google Gemini](https://aistudio.google.com/apikey) w zmiennej `GEMINI_API_KEYS` (rozdzielone przecinkami).

Wygenerowany raport: `index.html`

## Pliki

| Plik | Opis |
|------|------|
| `run.py` | Główny pipeline: scrape → classify → HTML |
| `urzedy.json` | Lista ~340 urzędów z URL-ami |
| `cache.json` | Cache klasyfikacji (URL → wynik) |
| `powiaty.geojson` | Granice powiatów do mapy |
| `urzad_to_powiat.json` | Mapowanie urząd → powiat GeoJSON |
| `index.html` | Wygenerowany raport (output) |

## Technologie

- Python 3.12 + requests + BeautifulSoup4
- Google Gemini 2.0 Flash API
- Leaflet.js (mapa)
- GitHub Actions (cron) + GitHub Pages (hosting)
