# Matrix Watcher

[Live dashboard](https://matrixwatcher.space) · [License](LICENSE) · [Author](https://moisejevs.com)

Matrix Watcher is an open experiment collecting real measurements and asking whether unusual observations in different domains coincide more often than a timing control suggests. It does **not** claim to predict earthquakes or markets. No cross-domain discovery has been independently validated.

## Twelve observed streams

| Stream | Data | Domain |
|---|---|---|
| Crypto | Binance BTC and ETH markets | markets |
| Blockchain | block timing from public nodes | blockchain |
| Quantum RNG | ANU hardware randomness when the API is available | quantum |
| Space weather | NOAA geomagnetic Kp and related readings | heliophysics |
| Solar wind | NOAA speed, density and interplanetary magnetic field | heliophysics |
| Solar activity | NOAA radio flux, X-rays and protons | heliophysics |
| Earthquakes | USGS seismic feed | geophysics |
| Volcanoes | Smithsonian/USGS activity reports | geophysics |
| Weather | Open-Meteo model estimates of temperature and pressure at the configured location, currently New York | atmosphere |
| News | newly seen headlines from four RSS feeds | human activity |
| Wikipedia | non-bot edits from Wikimedia EventStreams, with bounded history replay between polls | human activity |
| Earth tides | locally calculated gravitational tide; stored as context only | geophysics |

Three additional feeds are **context only**. The NASA/JPL CNEOS fireball catalogue can publish events days after they occurred; see the [NASA API description](https://ssd-api.jpl.nasa.gov/doc/fireball.html). RIPE Atlas measurements 1001 and 1004 ping K-root and F-root DNS servers from the same frozen panel of 18 public probes across Germany, the United States, Brazil, South Africa, Japan and Australia. The API can cache results for five minutes, and a connected probe may still have stale measurement results; the dashboard reports freshness for both targets. The paired targets help distinguish target issues from probe issues, but share the probes and API. The Weather Grid uses one batched Open-Meteo call for six fixed cities; its "current" values are [weather-model estimates](https://open-meteo.com/en/docs), not six independent station readings. None of these context feeds enters the anomaly detector or cluster count.

These are **not twelve independent domains**. Related solar feeds measure one physical chain. Earth tides are not an anomaly trigger. The live cluster level counts distinct domains within 30 seconds; a cluster is an observation, not a p-value or evidence of causation. A short window has limited power when streams are polled minutes apart.

## Processing and data quality

1. Sensors store timestamped raw JSONL. Missing core measurements cause collection errors; partial crypto, news and NOAA results carry quality metadata.
2. The live `HybridDetector` combines rolling robust deviations for continuous variables with named thresholds for physical events. A one-day USGS M4.5+ feed catches up after short collector outages; each earthquake is identified separately, acknowledged after storage and timed by its reported origin. The hourly aggregate still describes the past hour. News bursts use an attainable count of new headlines after the initial RSS baseline.
3. A single event-loop callback processes readings in order. It persists individual anomalies and domain-aware cluster records before optional pattern analysis. Exceptions are logged and published in `logs/pipeline_status.json`.
   The scheduler now measures configured intervals between the starts of polls; previously it added each request's duration, reducing actual sampling frequency. It still prevents the same sensor from overlapping itself.
4. The dashboard and `/api/coverage` show freshness and completeness of the latest stored reading for each source, plus pipeline health. They do not certify that every historical interval was observed.
5. Internal condition → event frequencies are exploratory. Repeated states, changing base rates and selection bias can inflate them. The public signal panel stays empty until a candidate has passed independent future-data validation. The former aggregate called a “Brier score” was not a proper forecast score and is no longer presented as one. A separate append-only Forecast Audit starts on 24 September 2026: five frozen historical candidate probabilities are issued prospectively at collector detection time, repeated source anomalies are collapsed into 30-minute episodes, binary outcomes require adequate target coverage, and actual Brier scores are compared with a frozen matched-timing baseline. Its scores are descriptive and cannot independently validate a signal.

The daily **Evidence Lab** counts distinct three-domain episodes at 30 seconds, 5 minutes and 15 minutes. Its control shifts whole domain event trains by complete days within each UTC month, preserving bursts, time of day and polling phase. Holm adjustment covers the three windows. The historical result remains **exploratory** because the method was refined after these data were seen. `/api/evidence` provides the report as JSON.

The **Discovery Workbench** adds two independent audits. The 30-day Observation Atlas scans raw records by UTC day, counts partial readings, and estimates how often hypothetical 30-second, 5-minute and 1-hour events could overlap a usable poll or Wikipedia observation interval. This is *sampling opportunity*, not measured detector power; it excludes source reporting lag and threshold misses. The Lag Lab collapses repeated anomalies into episodes and tests six directed, predeclared source pairs at two lag bands against within-month whole-day target shifts. Holm adjustment covers all twelve comparisons. The solar-wind → space-weather pair is a known-physics calibration; historical results remain exploratory. Earthquake anomaly timestamps use USGS origin time while most other streams use observation time, so apparent lags can arise from reporting delays. API: `/api/research/quality`, `/api/research/lags`, `/api/context/fireballs`.

The same twelve comparisons have a frozen **future-data screen**: UTC 2026-09-24 00:00 through 2027-01-22 00:00 (120 days), method `lag-v1-20260924`. No interim p-values are calculated. The first endpoint analysis is stored once in `logs/research/prospective-final.json`; daily reruns read that frozen file. Even a passing future screen would need source-coverage review and independent replication before a validated-signal claim.

The prospective Forecast Audit uses method `forecast-v1-20260924` over the same dates. It is a different test: forecasts are written before target outcomes, probabilities remain fixed throughout the study, and collection gaps mark negative outcomes unscorable. The solar-wind → Kp rule is a known-physics calibration. The forecast family's candidate probabilities were fit on pre-study exploratory history and may be miscalibrated; no result is promoted to the public validated-signal panel by the scoring code. API: `/api/research/forecast-audit`.

## September 2026 audit

The previous live pipeline had a `NameError` during probability calculation. Background-task exceptions were discarded, so cluster summaries failed to persist while individual anomalies continued to appear. News detection was disconnected from the live hybrid detector, and its target required 50 new headlines although the sensor read at most 40. One NOAA flare URL returned 404. Another NOAA parser read nonexistent solar-wind fields from an old row. Health could remain green while BTC was absent. Offline replay used different detector rules and fewer streams than live collection.

Those code faults have been repaired. A further live check found that ANU errors had silently switched the "quantum" sensor to Random.org atmospheric noise or local entropy. These substitute readings are now excluded from live analysis, replay and the public evidence report; older genuine ANU anomalies are retained only when their timestamps match raw ANU samples. Coverage exposes ANU outages. The production history remains incomplete; it cannot become clean prospective evidence retroactively. On the available post-May-2026 anomaly records, the exploratory analysis found no three-domain 30-second episode. Longer windows contain coincidences, but the September 23 comparison did not show a convincing excess after its timing control and three-window correction. This is **inconclusive**, especially with historical coverage gaps and low power at short timescales.

## Run and verify

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.json config.json
python -m pytest tests -q
python main.py
# In another shell:
python run_pwa.py
```

Collector health: `http://localhost:8080/health`. Dashboard: `http://localhost:5555/`. Change the collector health port with `MATRIX_WATCHER_HEALTH_PORT`. Check feed access and coverage before interpreting an empty result. Some sources require site-specific configuration.

```bash
# Rebuild derived observations without overwriting the live logs:
python -m src.analyzers.offline.replay --logs logs --out-dir /tmp/matrix-watcher-replay --dry-run
python -m src.analyzers.offline.evidence --logs logs/anomalies --out /tmp/matrix-watcher-evidence.json --days 120 --iterations 500
python -m src.analyzers.offline.quality_atlas --logs logs --out /tmp/matrix-watcher-quality.json --days 30
python -m src.analyzers.offline.lag_lab --logs logs/anomalies --out /tmp/matrix-watcher-lags.json --days 120 --iterations 500
```

The committed [tests](tests/) cover live detection, replay, domain grouping, NOAA parsing, news bursts, durable cluster records, coverage degradation and the new research reports. The `ops/systemd/` units provide a collector, PWA watchdog, evidence timer and research timer for the current server; change their paths for another host. Service output goes to the journal rather than unbounded application log files.

## Limits and research standard

- Wikipedia now resumes from the last persisted coverage endpoint using Wikimedia's `since` history stream. It waits for a small event-time watermark, deduplicates event IDs, excludes canary events and records exact covered intervals. Replay is capped at ten minutes so the public upstream is not burdened by a long outage. If that cap skips time, the reading is marked partial and withheld from live anomaly detection. Event-time ordering and late publication remain limitations; this is bounded replay, not a promise of perfect capture. The active weather detector still covers one location; the six-city model grid is context only. Cached or stale New York weather is excluded from anomaly detection. The Observation Atlas shows actual intervals measured from stored records.
- Public feeds can lag, change schema or fail. ANU's legacy public endpoint fails intermittently; a reliable quantum stream requires working ANU access, and no other entropy source will be labelled quantum. Sensor and pipeline status belong beside every apparent statistical finding.
- Nearby threshold crossings and repeated polls are not independent experiments. Evidence Lab counts overlap episodes; condition frequencies remain candidate descriptions.
- A predictive claim needs a frozen event definition and horizon, independent future episodes, a matched current base rate, calibration against binary outcomes and correction for all candidates tried.

The detection pipeline uses transparent statistical rules rather than generative AI. Its purpose is to make both unusual observations and failures of observation visible.

## Next instrument decisions

The [RIPE Atlas built-in measurements](https://atlas.ripe.net/docs/getting-started/built-in-measurements/) now cover two distinct root targets. A fixed 14-day reference period begins at UTC 2026-09-24 00:00 and freezes on 2026-10-08; it requires at least 100 unique measurements on ten days for two probes per region and target. The reference is never silently extended, and future deviations remain context until route changes, outages and maintenance are controlled. The six-city Weather Grid fixes coordinates, units and model-time freshness; it does not pretend the model values are independent sensors. A tested [two-station ingest protocol](docs/STATION_PROTOCOL.md) is ready for physical installation, calibration and a burn-in period. No hardware station is connected yet.
