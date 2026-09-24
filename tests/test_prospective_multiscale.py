import random
from datetime import datetime, timedelta, timezone

from src.analyzers.offline.prospective_multiscale import (
    END_AT, START_AT, build_report, eligible_observations,
    endpoint_analysis, rotate_eligible_days, usable_domain_days,
)


def _days(n=90):
    start = datetime.fromtimestamp(START_AT, timezone.utc).date()
    group = {start + timedelta(days=i) for i in range(n)}
    return {domain: set(group) for domain in ("markets", "blockchain", "geophysics")}


def test_interim_report_has_no_statistics_or_final_file(tmp_path):
    final = tmp_path / "frozen.json"
    report = build_report(START_AT + 30 * 86400, tmp_path, tmp_path, {}, final)
    assert report["status"] == "collecting_no_interim_test"
    assert "analysis" not in report and not final.exists()


def test_observation_clock_and_delayed_earthquake_filter():
    days = _days()
    records = [
        {"sensor_source": "crypto", "timestamp": START_AT + 100,
         "observed_at": START_AT + 110},
        {"sensor_source": "earthquake", "timestamp": START_AT + 100,
         "observed_at": START_AT + 2000, "source_event_at": START_AT + 100},
        {"sensor_source": "volcanic_activity", "observed_at": START_AT + 110},
        {"sensor_source": "blockchain", "timestamp": START_AT + 100},
    ]
    events, excluded = eligible_observations(records, days)
    assert events == [(START_AT + 110, "markets")]
    assert excluded == {"late_or_unknown_earthquake_origin": 1,
                        "missing_observation_clock": 1}


def test_domain_day_requires_every_member_stream(tmp_path, monkeypatch):
    import src.analyzers.offline.prospective_multiscale as study

    monkeypatch.setattr(study, "scan_day", lambda path, source, interval, start:
                        {"poll_coverage": 0.0 if source == "wikipedia_edits" else 0.5})
    days = usable_domain_days(tmp_path, {})
    assert len(days["markets"]) == 120
    assert days["human_activity"] == set()


def test_insufficient_coverage_has_no_p_values():
    days = _days(3)
    events = [(START_AT + 100 + i, domain) for i, domain in enumerate(days)]
    report = endpoint_analysis(events, days, iterations=5)
    assert report["status"] == "insufficient_coverage_or_events_no_test"
    assert "windows" not in report


def test_rotated_day_profiles_stay_on_eligible_days_and_keep_time_of_day():
    days = _days()
    events = [(START_AT + i * 86400 + 100, domain)
              for i in range(5) for domain in days]
    shifted = rotate_eligible_days(events, days, random.Random(42))
    assert len(shifted) == len(events)
    assert all(datetime.fromtimestamp(ts, timezone.utc).date() in days[domain]
               and ts % 86400 == 100 for ts, domain in shifted)


def test_endpoint_is_explicitly_exploratory_and_adjusts_all_windows():
    days = _days()
    events = [(START_AT + i * 86400 + 100 + j, domain)
              for i in range(5)
              for j, domain in enumerate(("markets", "blockchain", "geophysics"))]
    report = endpoint_analysis(events, days, iterations=20)
    assert report["status"] == "exploratory_endpoint_screen_not_validated"
    assert [row["seconds"] for row in report["windows"]] == [30, 300, 900]
    assert all(row["p_holm"] >= row["p_value"] for row in report["windows"])
    assert END_AT == START_AT + 120 * 86400
