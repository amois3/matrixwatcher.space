import random

from src.analyzers.offline.evidence import analyze, count_episodes, domain_events, shift_within_months


def test_episodes_count_once_and_group_related_solar_feeds():
    records = [
        {"timestamp": 1780300000.0, "sensor_source": "solar_wind"},
        {"timestamp": 1780300001.0, "sensor_source": "space_weather"},
        {"timestamp": 1780300002.0, "sensor_source": "solar_activity"},
        {"timestamp": 1780300003.0, "sensor_source": "crypto"},
        {"timestamp": 1780300004.0, "sensor_source": "earthquake"},
        {"timestamp": 1780300005.0, "sensor_source": "crypto"},
    ]
    events = domain_events(records)
    assert count_episodes(events, 30) == 1
    assert analyze(events, iterations=10)["status"] == "exploratory_not_validated"


def test_historical_wikimedia_source_is_not_an_independent_domain():
    records = [
        {"timestamp": 1780300000.0, "sensor_source": "news"},
        {"timestamp": 1780300001.0, "sensor_source": "wikimedia_edits"},
        {"timestamp": 1780300002.0, "sensor_source": "crypto"},
    ]
    events = domain_events(records)
    assert {domain for _, domain in events} == {"human_activity", "markets"}
    assert count_episodes(events, 30) == 0


def test_month_shift_preserves_domain_counts_and_time_of_day():
    events = sorted((1780300000.0 + day * 86400, domain)
                    for day in range(10) for domain in ("markets", "geophysics"))
    shifted = shift_within_months(events, random.Random(4))
    assert len(shifted) == len(events)
    assert sorted(domain for _, domain in shifted) == sorted(domain for _, domain in events)
    assert all((ts % 86400) == (events[0][0] % 86400) for ts, _ in shifted)
