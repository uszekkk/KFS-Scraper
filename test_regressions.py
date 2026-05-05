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

    def test_budget_forecast_flags_remaining_limit_as_hot(self):
        results = [
            {
                "urzad": "Testowo",
                "title": "KFS 2026",
                "snippet": "Dostepna na dany rok kwota srodkow KFS wynosi 1 000 000 zl.",
                "url": "https://example.com/kfs",
                "wynik": "NIE",
                "termin": "",
                "kwota": "",
            },
            {
                "urzad": "Testowo",
                "title": "Nabor wnioskow KFS",
                "snippet": "Urzad oglasza nabor KFS.",
                "url": "https://example.com/nabor",
                "wynik": "TAK",
                "termin": "01.03.2026 - 05.03.2026",
                "kwota": "300 000 zl",
            },
        ]

        forecast = run.build_budget_forecasts(results)[0]

        self.assertEqual(forecast["level"], "HOT")
        self.assertEqual(forecast["limit_roczny"], 1000000)
        self.assertEqual(forecast["suma_naborow"], 300000)
        self.assertEqual(forecast["szacowane_pozostalo"], 700000)
        self.assertTrue(forecast["ask_public_info"])

    def test_budget_forecast_exhausted_signal_overrides_gap(self):
        results = [
            {
                "urzad": "Testowo",
                "title": "KFS 2026",
                "snippet": "Dostepna na dany rok kwota srodkow KFS wynosi 1 000 000 zl.",
                "url": "https://example.com/kfs",
                "wynik": "NIE",
                "termin": "",
                "kwota": "",
            },
            {
                "urzad": "Testowo",
                "title": "Informacja KFS",
                "snippet": "Srodki KFS zostaly wyczerpane i rozdysponowane w calosci.",
                "url": "https://example.com/koniec",
                "wynik": "NIE",
                "termin": "",
                "kwota": "",
            },
        ]

        forecast = run.build_budget_forecasts(results)[0]

        self.assertEqual(forecast["level"], "COLD")
        self.assertTrue(forecast["has_exhausted_signal"])

    def test_budget_forecast_does_not_treat_until_exhaustion_rule_as_exhausted(self):
        norm = run._ascii_lower(
            "Nabor wnioskow jest powtarzany do wyczerpania limitu srodkow KFS."
        )

        self.assertFalse(run._has_exhausted_signal(norm))


if __name__ == "__main__":
    unittest.main()
