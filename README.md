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
| Weather | temperature and pressure at the configured location, currently New York | atmosphere |
| News | newly seen headlines from four RSS feeds | human activity |
| Wikipedia | non-bot edits during an 8-second sample of each roughly 60-second poll | human activity |
| Earth tides | locally calculated gravitational tide; stored as context only | geophysics |

These are **not twelve independent domains**. Related solar feeds measure one physical chain. Earth tides are not an anomaly trigger. The live cluster level counts distinct domains within 30 seconds; a cluster is an observation, not a p-value or evidence of causation. A short window has limited power when streams are polled minutes apart.

## Processing and data quality

1. Sensors store timestamped raw JSONL. Missing core measurements cause collection errors; partial crypto, news and NOAA results carry quality metadata.
2. The live `HybridDetector` combines rolling robust deviations for continuous variables with named thresholds for physical events. A one-day USGS M4.5+ feed catches up after short collector outages; each earthquake is identified separately, acknowledged after storage and timed by its reported origin. The hourly aggregate still describes the past hour. News bursts use an attainable count of new headlines after the initial RSS baseline.
3. A single event-loop callback processes readings in order. It persists individual anomalies and domain-aware cluster records before optional pattern analysis. Exceptions are logged and published in `logs/pipeline_status.json`.
4. The dashboard and `/api/coverage` show freshness and completeness of the latest stored reading for each source, plus pipeline health. They do not certify that every historical interval was observed.
5. Internal condition → event frequencies are exploratory. Repeated states, changing base rates and selection bias can inflate them. The public signal panel stays empty until a candidate has passed independent future-data validation. The former aggregate called a “Brier score” was not a proper forecast score and is no longer presented as one.

The daily **Evidence Lab** counts distinct three-domain episodes at 30 seconds, 5 minutes and 15 minutes. Its control shifts whole domain event trains by complete days within each UTC month, preserving bursts, time of day and polling phase. Holm adjustment covers the three windows. The historical result remains **exploratory** because the method was refined after these data were seen. `/api/evidence` provides the report as JSON.

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
```

The committed [tests](tests/) cover live detection, replay, domain grouping, NOAA parsing, news bursts, durable cluster records and coverage degradation. The `ops/systemd/` units provide a collector, PWA watchdog and daily evidence timer for the current server; change their paths for another host. Service output goes to the journal rather than unbounded application log files.

## Limits and research standard

- Wikipedia samples only part of each interval; brief edit bursts can be missed. Weather currently covers one location.
- Public feeds can lag, change schema or fail. ANU's legacy public endpoint fails intermittently; a reliable quantum stream requires working ANU access, and no other entropy source will be labelled quantum. Sensor and pipeline status belong beside every apparent statistical finding.
- Nearby threshold crossings and repeated polls are not independent experiments. Evidence Lab counts overlap episodes; condition frequencies remain candidate descriptions.
- A predictive claim needs a frozen event definition and horizon, independent future episodes, a matched current base rate, calibration against binary outcomes and correction for all candidates tried.

The detection pipeline uses transparent statistical rules rather than generative AI. Its purpose is to make both unusual observations and failures of observation visible.
