import os
import sys
import types
import unittest

os.environ.setdefault("GEMINI_API_KEYS", "dummy")

if "requests" not in sys.modules:
    requests_stub = types.ModuleType("requests")

    class DummySession:
        def __init__(self):
            self.headers = {}

        def get(self, *args, **kwargs):
            raise requests_stub.RequestException("network disabled in unit tests")

    requests_stub.Session = DummySession
    requests_stub.RequestException = Exception
    requests_stub.get = lambda *args, **kwargs: (_ for _ in ()).throw(
        requests_stub.RequestException("network disabled in unit tests")
    )
    requests_stub.post = requests_stub.get
    requests_stub.put = requests_stub.get
    sys.modules["requests"] = requests_stub

if "bs4" not in sys.modules:
    bs4_stub = types.ModuleType("bs4")
    bs4_stub.BeautifulSoup = lambda *args, **kwargs: None
    sys.modules["bs4"] = bs4_stub

import run


class RegressionTests(unittest.TestCase):
    def test_rule_based_detects_obvious_kfs_call(self):
        art = {
            "title": "Nabor wnioskow KFS 2026",
            "snippet": (
                "Powiatowy Urzad Pracy oglasza nabor wnioskow pracodawcow o srodki KFS. "
                "Wnioski beda przyjmowane od 13.05.2026 r. do 19.05.2026 r. "
                "Limit srodkow: 1 412 100,00 zl."
            ),
            "url": "https://example.com/-/nabor-kfs",
            "urzad": "Powiatowy Urzad Pracy w Klobucku",
        }

        result = run.rule_based_classify_kfs(art)

        self.assertIsNotNone(result)
        self.assertEqual(result["wynik"], "TAK")
        self.assertEqual(result["termin"], "13.05.2026 - 19.05.2026")
        self.assertIn("1 412 100,00 zl", result["kwota"])

    def test_bad_empty_cache_for_strong_kfs_is_refreshed(self):
        art = {
            "title": "Nabor wnioskow KFS",
            "snippet": "Oglasza sie nabor wnioskow pracodawcow o srodki KFS od 15.05.2026 do 22.05.2026.",
            "url": "https://example.com/-/nabor-kfs",
        }
        cached = {"wynik": "NIE", "powod": "", "termin": "", "kwota": ""}

        self.assertTrue(run._cache_needs_refresh(art, cached))

    def test_rule_prefers_current_call_remaining_amount(self):
        art = {
            "title": "Ogloszenie o II naborze wnioskow KFS 2026",
            "snippet": (
                "Urzad rozpoczyna II nabor wnioskow o srodki KFS. "
                "Urzad posiada srodki w wysokosci 1\u00a0884\u00a0021,05 zl "
                "(w ramach obecnego naboru pozostalo do rozdysponowania 467\u00a0729,25 zl). "
                "Nabor od 15.05.2026 r. do 22.05.2026 r."
            ),
            "url": "https://example.com/olsztyn",
        }

        result = run.rule_based_classify_kfs(art)

        self.assertEqual(result["kwota"], "467 729,25 zl")

    def test_expand_multi_nabory_splits_ranges_and_amounts(self):
        results = [{
            "wynik": "TAK",
            "powod": "AI",
            "termin": "23.03.2026 - 27.03.2026 oraz 08.06.2026 - 12.06.2026",
            "kwota": "650 000 zl oraz 327 100 zl",
            "title": "Nabor KFS",
            "url": "https://example.com/puck",
        }]

        expanded = run.expand_multi_nabory(results)

        self.assertEqual(len(expanded), 2)
        self.assertEqual(expanded[0]["termin"], "23.03.2026 - 27.03.2026")
        self.assertEqual(expanded[1]["termin"], "08.06.2026 - 12.06.2026")
        self.assertEqual(expanded[0]["kwota"], "650 000 zl")
        self.assertEqual(expanded[1]["kwota"], "327 100 zl")

    def test_nabory_key_allows_multiple_terms_same_city(self):
        first = run.make_nabory_key("Powiatowy Urzad Pracy w Kartuzach", "2026-06-15", "2026-06-19", 600000)
        second = run.make_nabory_key("PUP Kartuzy", "2026-08-31", "2026-09-04", 578300)

        self.assertNotEqual(first, second)
        self.assertEqual(first[0], second[0])


if __name__ == "__main__":
    unittest.main()
