#!/usr/bin/env python3
"""
Jednorazowy skrypt deduplikacji NaboryKfs w EspoCRM.

Reguła: jeśli (urzad, termin, kwota) się powtarzają — to jest ten sam nabór
opisany w wielu artykułach (przypomnienia, spotkania itp.).
Zostawiamy najstarszy rekord (po datapublikacji / createdAt), resztę usuwamy.
"""

import json
import os
import sys
from collections import defaultdict

import requests

CRM_URL = os.environ.get("ESPOCRM_URL", "").rstrip("/")
CRM_KEY = os.environ.get("ESPOCRM_API_KEY", "")

if not CRM_URL or not CRM_KEY:
    print("BŁĄD: Ustaw ESPOCRM_URL i ESPOCRM_API_KEY")
    sys.exit(1)

HEADERS = {
    "X-Api-Key": CRM_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def get_all_records():
    """Pobiera wszystkie NaboryKfs z CRM."""
    records = []
    offset = 0
    page_size = 200
    while True:
        resp = requests.get(
            f"{CRM_URL}/api/v1/NaboryKfs",
            headers=HEADERS,
            params={
                "select": "id,name,urzad,termin,kwota,url,datapublikacji,createdAt,status",
                "maxSize": page_size,
                "offset": offset,
            },
            timeout=15,
        )
        if not resp.ok:
            print(f"Błąd HTTP {resp.status_code}")
            break
        page = resp.json().get("list", [])
        records.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return records


def delete_record(record_id):
    """Usuwa rekord NaboryKfs z CRM."""
    resp = requests.delete(
        f"{CRM_URL}/api/v1/NaboryKfs/{record_id}",
        headers=HEADERS,
        timeout=15,
    )
    return resp.ok


def make_dedup_key(rec):
    """Klucz deduplikacji: (urzad, termin, kwota). Jeśli brak — None (nie deduplikuj)."""
    urzad = (rec.get("urzad") or "").strip().lower()
    termin = (rec.get("termin") or "").strip().lower()
    kwota = (rec.get("kwota") or "").strip().lower()

    # Wymaga co najmniej urzad + (termin lub kwota)
    if not urzad:
        return None
    if not termin and not kwota:
        return None

    return (urzad, termin, kwota)


def get_sort_date(rec):
    """Zwraca datę do sortowania — najstarsza wygrywa."""
    # Preferuj datapublikacji, fallback na createdAt
    d = rec.get("datapublikacji") or rec.get("createdAt") or "9999-99-99"
    return d


def main():
    print(f"Pobieranie NaboryKfs z {CRM_URL}...")
    records = get_all_records()
    print(f"Pobrano {len(records)} rekordów\n")

    # Grupuj po kluczu deduplikacji
    groups = defaultdict(list)
    no_key = 0
    for rec in records:
        key = make_dedup_key(rec)
        if key is None:
            no_key += 1
            continue
        groups[key].append(rec)

    # Znajdź duplikaty
    to_delete = []
    for key, recs in groups.items():
        if len(recs) <= 1:
            continue

        # Sortuj — najstarszy pierwszy (zostawiamy go)
        recs.sort(key=get_sort_date)
        keep = recs[0]
        dupes = recs[1:]

        print(f"DUPLIKAT: urząd={key[0]}, termin={key[1]}, kwota={key[2]}")
        print(f"  ZOSTAWIAM: [{keep['id']}] {keep.get('name','')[:60]} (data: {get_sort_date(keep)})")
        for d in dupes:
            print(f"  USUWAM:    [{d['id']}] {d.get('name','')[:60]} (data: {get_sort_date(d)})")
            to_delete.append(d)
        print()

    print(f"{'=' * 60}")
    print(f"Rekordów łącznie:      {len(records)}")
    print(f"Bez klucza (pominięte): {no_key}")
    print(f"Grup z duplikatami:     {sum(1 for recs in groups.values() if len(recs) > 1)}")
    print(f"Do usunięcia:           {len(to_delete)}")
    print(f"Zostanie:               {len(records) - len(to_delete)}")

    if not to_delete:
        print("\nBrak duplikatów — nic do usunięcia.")
        return

    # Potwierdzenie
    answer = input(f"\nCzy usunąć {len(to_delete)} duplikatów? (tak/nie): ").strip().lower()
    if answer != "tak":
        print("Anulowano.")
        return

    # Usuwanie
    deleted = 0
    errors = 0
    for rec in to_delete:
        if delete_record(rec["id"]):
            deleted += 1
            print(f"  ✓ Usunięto {rec['id']}")
        else:
            errors += 1
            print(f"  ✗ Błąd usuwania {rec['id']}")

    print(f"\nGotowe: usunięto {deleted}, błędów {errors}")


if __name__ == "__main__":
    main()
