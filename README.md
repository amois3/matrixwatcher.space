# Matrix Watcher

[Live dashboard](https://matrixwatcher.space) · [License](LICENSE) · [Author](https://moisejevs.com)

Matrix Watcher is an open observation experiment. It records public measurements, checks whether each source was available, flags unusual readings, and asks whether events in different domains coincide more often than timing controls suggest. It does not establish that reality is a simulation, explain a coincidence, or offer a validated forecast. No cross-domain discovery has been independently confirmed.

## What is running

The live configuration monitors **16 sources**: 11 can produce anomaly events, one is a calculated covariate, and four provide context. Counts are sources, not independent experiments. Related feeds can share a physical process or an upstream API.

| Source | Observation | Role / domain |
| --- | --- | --- |
| Crypto | Binance BTC and ETH market data | Anomaly / markets |
| Blockchain | Block timing from public nodes | Anomaly / blockchain |
| Quantum RNG | ANU hardware random samples, only when ANU responds | Anomaly / quantum |
| Space weather | NOAA geomagnetic Kp and related values | Anomaly / heliophysics |
| Solar wind | NOAA speed, density and interplanetary magnetic field | Anomaly / heliophysics |
| Solar activity | NOAA radio flux, X-rays and protons | Anomaly / heliophysics |
| Earthquakes | USGS seismic feed | Anomaly / geophysics |
| Volcanoes | Smithsonian/USGS activity reports | Anomaly / geophysics |
| Weather | Open-Meteo **model estimate** for New York | Anomaly / atmosphere |
| News | Newly seen headlines from four public RSS feeds | Anomaly / human activity |
| Wikipedia | Non-bot edits from Wikimedia EventStreams | Anomaly / human activity |
| Earth tides | Locally calculated gravitational tide | Covariate / geophysics; no anomaly vote |
| NASA fireballs | [CNEOS catalogue](https://ssd-api.jpl.nasa.gov/doc/fireball.html), sometimes published days late | Context / astronomy |
| RIPE Atlas | Paired K-root and F-root pings from a fixed panel of 18 public probes | Context / network |
| Weather Grid | Six-city [Open-Meteo model](https://open-meteo.com/en/docs) estimates | Context / atmosphere |
| Global Stations | Physical airport [METAR observations](https://aviationweather.gov/data/api/) in six world regions | Context / atmosphere |

The model grid covers New York, Riga, São Paulo, Cape Town, Tokyo and Sydney. The physical station panel uses nearby airports KJFK, EVRA, SBGR, FACT, RJTT and YSSY. The METAR feed makes one batch request every ten minutes. It stores each source observation time and raw report, validates station identity, location, age and values, and shows missing or stale reports. A repeated poll of one METAR is still one observation. An airport altimeter setting is not the same measurement as model surface pressure, and airport locations differ from city centres. These weather context feeds are **not** six extra anomaly votes.

### Earthquakes: observations, not advance predictions

The USGS feed collects global M4.5+ events. The live detector records named earthquake anomalies and the coverage panel shows whether collection is working. A legacy pattern tracker also stored frequencies of M6+ events following other detected conditions. It used the most common region among matched *past* events as a label. The one-hour aggregate could repeat the same earthquake on successive polls, so these matched-condition counts are not independent earthquake outcomes. This is not a trained model of the next earthquake's location: it has no frozen geographic/time/magnitude target, prospectively scored hits and misses, calibrated probability, or comparison with a seismic base rate. The legacy tracker no longer updates seismic associations or emits earthquake forecast entries; the historical records remain for exploratory audit. The public Validated Signals panel has no earthquake forecast.

To study spatial seismic risk honestly, a new protocol would first fix regions, a magnitude threshold, horizon, forecast issuance times and a historical seismicity baseline. It would log probability for **every region and interval before the outcome**, score all misses as well as hits on future complete data, and compare skill with the baseline over a sufficiently long period. Such a probability forecast would still not identify the time, place and magnitude of the next individual earthquake. See the [USGS explanation](https://www.usgs.gov/faqs/can-you-predict-earthquakes).

## From a reading to a finding

1. The collector polls or streams enabled sources and stores timestamped raw JSONL readings. Failed and partial collections are surfaced through [coverage](https://matrixwatcher.space/api/coverage), rather than silently interpreted as normal. Newly stored anomalies keep separate `source_event_at`, `published_at`, `observed_at` and `detected_at` fields. Unknown upstream times remain null. An earthquake's USGS origin is an event time; a volcano feed's weekly date is a publication time, not eruption onset. The legacy `timestamp` is retained for older analyses, not silently rewritten. The dashboard's online indicator only shows that its API answered; use the coverage panel to judge the measurements.
2. The live `HybridDetector` uses rolling robust deviations where appropriate and explicit thresholds for named physical events. USGS earthquakes are identified individually, news bursts count newly seen headlines, and a genuine ANU sample is required for the quantum stream. Cached or substitute data do not become quantum or weather anomalies.
3. The live cluster detector groups anomalies within **30 seconds** and counts distinct domains. Solar activity, solar wind and geomagnetic Kp count as one heliophysics domain; news and Wikipedia share human activity. The level describes the number of domains observed in a time window. It is **not** a p-value, a proof of causation, or a calibrated probability.
4. The daily **Evidence Lab** also examines distinct three-domain episodes within 30 seconds, 5 minutes and 15 minutes. It compares them with within-month whole-day shifts of domain event trains and adjusts across those three windows. The historical analysis is exploratory because the method was refined after seeing historical data. [Current report](https://matrixwatcher.space/api/evidence).
5. The **Observation Atlas** audits the previous 30 complete UTC days of raw records. Its selectable 30-second, 5-minute, 15-minute and 1-hour durations estimate when a hypothetical event could overlap a usable poll or stream interval. They do not measure detector power, joint multi-source availability or upstream reporting delay. The **Lag Lab** compares six predeclared directed pairs at two lag bands against a shifted-time control and adjusts all twelve comparisons. A solar-wind → geomagnetic pair is a known-physics check, not a new discovery. [Atlas](https://matrixwatcher.space/api/research/quality) · [Lag Lab](https://matrixwatcher.space/api/research/lags).
6. Two studies use **future observations from 2026-09-24 00:00 to 2027-01-22 00:00 UTC**. The lag screen freezes its twelve comparisons and does not issue interim p-values. The append-only [Forecast Audit](https://matrixwatcher.space/api/research/forecast-audit) records five frozen candidate probabilities before their target outcomes and compares binary Brier scores with a frozen matched-timing baseline. A negative outcome is scored only with adequate target coverage. Neither study automatically promotes a rule to the public **Validated Signals** panel; endpoint review and independent replication are still required.

The RIPE Atlas K-root/F-root panel has its own fixed 14-day reference period from 2026-09-24 to 2026-10-08 UTC. Both targets share probes and an API, so a change remains network context until outages, routes and maintenance are considered.

### Future observation-time coincidence screen

A separate, versioned [screen](https://matrixwatcher.space/api/research/prospective-multiscale) is frozen before its first eligible day, **2026-09-25 00:00 to 2027-01-23 00:00 UTC**. It does not change the September 24 lag or forecast protocols. It tests 30 seconds, 5 minutes and 15 minutes using only newly recorded collector `observed_at` clocks, counts distinct three-domain episodes and applies Holm adjustment across all three windows. Weekly volcanic reports and context feeds are excluded; earthquakes detected more than 15 minutes after their USGS origin and unverified quantum samples are excluded. This tests coincidences in *observations*, not simultaneous physical causes.

At the endpoint, a domain-day is eligible only when every included stream in that domain has at least 30% usable scheduled polling bins. At least 90 of 120 eligible days are required for a domain; at least three domains and four eligible days per domain/month are needed for a timing comparison. The 2,000 fixed-seed controls rotate each domain's complete within-day event profiles only among its eligible UTC days in the same month. This preserves the within-day bursts and time of day while avoiding control events on whole missing days. No interim p-values are produced. The final report is frozen once written and remains an **exploratory screen**, even if it finds an excess: day-level eligibility does not correct within-day outages, feed publication delays, shared upstream failures or every temporal trend. Independent replication and review of the full source-quality history remain mandatory.

## What to expect

The September 2026 audit repaired a broken cluster-processing path, disconnected news detection, NOAA parser failures, slow scheduling and a quantum fallback that had mislabeled non-ANU randomness. The repaired pipeline gives a better chance of **observing and correctly classifying** events that the earlier implementation could miss. Coverage and explicit gaps make a quiet result easier to interpret. They do not increase the prevalence of real unexplained phenomena.

The global weather stations, six-city model grid, NASA catalogue and RIPE probes broaden what the site can show, but they are context only and **do not raise the live cluster count**. The 30-second window still has limited power when some sources update only every few minutes or publish late. The Observation Atlas and wider-window research reports expose that limitation. Adding sensors without a predeclared hypothesis, local baselines and controls would mainly add chances for false positives.

Historical production data were not retroactively made complete by fixing the code. The available post-May-2026 anomaly history showed no three-domain 30-second episode in the 2026-09-23 exploratory report; longer-window coincidences did not show a convincing excess after the timing control and correction. This is **inconclusive**, not evidence that no relationship can exist. The dashboard reports current status; consult the dated API reports before interpreting any result.

## Run locally

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

Collector health is at `http://localhost:8080/health`; the dashboard is at `http://localhost:5555/`. Set `MATRIX_WATCHER_HEALTH_PORT` to change the collector health port. Inspect `/api/coverage` and source access before reading a quiet dashboard as a null result. Production uses the user-level units in [`ops/systemd/`](ops/systemd/), including the collector, PWA watchdog, evidence timer and research timer. Service output is in the systemd journal. The code is public; production observation logs are not bundled with this repository.

For an offline analysis without overwriting live logs:

```bash
python -m src.analyzers.offline.replay --logs logs --out-dir /tmp/matrix-watcher-replay --dry-run
python -m src.analyzers.offline.evidence --logs logs/anomalies --out /tmp/matrix-watcher-evidence.json --days 120 --iterations 500
python -m src.analyzers.offline.quality_atlas --logs logs --out /tmp/matrix-watcher-quality.json --days 30
python -m src.analyzers.offline.lag_lab --logs logs/anomalies --out /tmp/matrix-watcher-lags.json --days 120 --iterations 500
```

## Scientific limits

- An empty result is informative only to the extent that source coverage, sampling opportunity and detector sensitivity are known. ANU's public endpoint can fail; other entropy must not be labelled quantum. Wikimedia replay is bounded to ten minutes, and longer gaps are marked incomplete.
- Reporting times differ by source. USGS earthquake origin time and collector detection time are different clocks. The forecast diary uses collector detection time so a late report cannot create a forecast in the past.
- The new observation-time screen starts with newly timestamped future records. Its daily coverage gate and whole-day timing control reduce known biases but cannot establish physical simultaneity or causation. Its thresholds were fixed before its first eligible day; changing them creates a new version and future start.
- Nearby threshold crossings and repeated polls are not independent experiments. A candidate association needs a frozen definition, matched base rate, independent future episodes, uncertainty, calibration and correction for all tried candidates.
- A future weather anomaly study would need station-specific hour-of-day and seasonal baselines, source-time alignment, consistent physical quantities and multiple-testing controls. The current global station panel makes no anomaly claim.

Matrix Watcher uses transparent detection rules rather than a generative model. Its purpose is to make both unusual observations and failures of observation visible.
