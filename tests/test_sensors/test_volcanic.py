"""Tests for the volcanic activity RSS parser — no network."""

from src.sensors.volcanic_activity_sensor import parse_volcano_feed


SAMPLE = """<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Smithsonian / USGS Weekly Volcanic Activity Report</title>
    <pubDate>Thu, 21 May 2026 05:11:06 -0500</pubDate>
    <item>
      <title>Ambae (Vanuatu) - Report for 14 May-20 May 2026 - New Eruptive Activity</title>
      <description>some text</description>
    </item>
    <item>
      <title>Bulusan (Philippines) - Report for 14 May-20 May 2026 - New Unrest</title>
    </item>
    <item>
      <title>Dukono (Indonesia) - Report for 14 May-20 May 2026 - Continuing Eruptive Activity</title>
    </item>
    <item>
      <title>Taal (Philippines) - Report for 14 May-20 May 2026 - New Eruptive Activity</title>
    </item>
  </channel>
</rss>
"""


def test_parse_volcano_feed_counts_categories():
    s = parse_volcano_feed(SAMPLE)
    assert s["active_count"] == 4
    assert s["new_eruptions"] == 2          # Ambae + Taal
    assert s["new_unrest"] == 1             # Bulusan
    assert s["continuing_eruptions"] == 1   # Dukono


def test_parse_volcano_feed_extracts_names():
    s = parse_volcano_feed(SAMPLE)
    assert set(s["volcanoes"]) == {"Ambae", "Bulusan", "Dukono", "Taal"}


def test_parse_volcano_feed_pubdate_unix():
    s = parse_volcano_feed(SAMPLE)
    # 21 May 2026 05:11:06 -0500 -> 10:11:06 UTC -> known epoch
    expected = 1779358266.0  # 2026-05-21T10:11:06Z
    assert abs(s["feed_pub_date_unix"] - expected) < 5.0


def test_parse_volcano_feed_garbage_returns_empty_summary():
    s = parse_volcano_feed("not xml at all")
    assert s["active_count"] == 0
    assert s["volcanoes"] == []


def test_parse_volcano_feed_empty_channel():
    s = parse_volcano_feed('<?xml version="1.0"?><rss version="2.0"><channel/></rss>')
    assert s["active_count"] == 0
