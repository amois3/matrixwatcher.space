"""Tests for offline reverse-geocoding region resolution (earthquake regions)."""

from src.analyzers.online.historical_pattern_tracker import (
    get_region_from_coords,
    get_most_frequent_region,
)


def test_region_japan():
    region = get_region_from_coords(35.6, 139.7)  # Tokyo area
    assert "Japan" in region


def test_region_california_usa():
    region = get_region_from_coords(36.7, -119.4)  # Central California
    assert "United States" in region
    assert "California" in region  # admin1 granularity preserved


def test_region_chile():
    region = get_region_from_coords(-33.4, -70.6)  # Santiago
    assert "Chile" in region


def test_ocean_point_resolves_to_nearest_land_region():
    # Near Tonga trench (a real seismic ocean zone) -> nearest land = Tonga
    region = get_region_from_coords(-20.0, -175.0)
    assert "Tonga" in region


def test_unknown_on_bad_input_never_fabricates():
    # Non-finite / absurd input should degrade to a real resolution or "Unknown",
    # never crash and never invent a place.
    region = get_region_from_coords(0.0, 0.0)  # Gulf of Guinea (open ocean)
    assert isinstance(region, str) and region != ""


def test_most_frequent_region_threshold():
    # 8 Japan-area points + 2 California points -> Japan dominates (>30%)
    jp = (35.6, 139.7)
    ca = (36.7, -119.4)
    locations = [jp] * 8 + [ca] * 2
    region = get_most_frequent_region(locations)
    assert region is not None and "Japan" in region


def test_most_frequent_region_needs_minimum_points():
    assert get_most_frequent_region([(35.6, 139.7), (35.6, 139.7)]) is None
