<div align="center">

# 🛰️ Matrix Watcher

### A rigorously honest monitor for hidden correlations across independent real-world systems

[![Live](https://img.shields.io/badge/live-matrixwatcher.space-00d4ff?style=flat-square)](https://matrixwatcher.space)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg?style=flat-square)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-3776ab?style=flat-square&logo=python&logoColor=white)](https://www.python.org)
[![Status](https://img.shields.io/badge/status-running%2024%2F7-00ff88?style=flat-square)](https://matrixwatcher.space)
[![No AI](https://img.shields.io/badge/analysis-pure%20statistics-aa66ff?style=flat-square)](#-no-ai-pure-statistics)

**[🌐 Live dashboard → matrixwatcher.space](https://matrixwatcher.space)**

*We watch. We measure. We tell the truth — even when the truth is "nothing here."*

</div>

---

## What is Matrix Watcher?

Matrix Watcher watches **9 completely independent real-world data streams** at once — from Bitcoin to earthquakes to hardware quantum noise — and asks a single, honest question:

> **Do anomalies in unrelated domains line up more often than pure chance would produce?**

It is a **measurement instrument, not a fortune teller.** It does not promise to predict markets or earthquakes. It records what happens, tests it against chance with methods designed to *disprove* any apparent pattern, and publishes the result transparently — including the unglamorous but valuable answer: *no significant signal so far.*

That honesty is the point. Most "correlation" projects fool themselves (or you). Matrix Watcher is built to be impossible to fool — by itself or anyone else.

---

## 📡 The 9 data sources

Every source is a **real, public data feed** — no simulations, no fabricated numbers.

| # | Source | What it tracks | Feed |
|---|--------|----------------|------|
| 💰 | **Crypto** | BTC/ETH price moves & volatility | Binance |
| ⛓️ | **Blockchain** | Network block times & on-chain anomalies | public RPC |
| 🎲 | **Quantum RNG** | Hardware quantum randomness | ANU QRNG |
| 🛰️ | **Space Weather** | Geomagnetic Kp index, solar wind | NOAA SWPC |
| ☀️ | **Solar Activity** | F10.7 flux, GOES X-ray flares, proton flux | NOAA |
| 🌍 | **Earthquakes** | Global seismicity (magnitude, location) | USGS |
| 🌋 | **Volcanoes** | Weekly volcanic activity report | Smithsonian / USGS |
| 🌦️ | **Weather** | Temperature & pressure swings | Open-Meteo |
| 📰 | **News** | Global headline volume | public RSS |

---

## ⚙️ How it works

```
9 live sensors  →  edge-triggered anomaly detection  →  30-second correlation clusters
                →  anomaly index  →  honest skill-filtered estimates  →  live dashboard + daily digest
```

1. **Sensing** — each source is polled in real time and stored as raw JSONL.
2. **Anomaly detection** — a reading crossing its threshold becomes an *anomaly*, emitted **once on the rising edge** (a single ongoing event is never double-counted).
3. **Clustering** — when anomalies from several *independent* sources land in the same 30-second window, that is a cluster. The level (1–5) is simply *how many distinct domains coincided* — a temporal coincidence, never a claim of causation.
4. **Honest estimates** — observed frequencies are shown **only when they beat the event's base rate** (`skill = P(event│condition) − P(event)`). No edge over chance → nothing is shown.

| Level | Meaning |
|-------|---------|
| **L1** | single source (background) |
| **L2** | two domains coincide |
| **L3** | three domains — *Multiple Correlation* |
| **L4** | four domains — *Strong Correlation* |
| **L5** | five or more — *Critical Synchronicity* (rarest) |

---

## 🔬 How we avoid fooling ourselves

This is the heart of the project. Apparent patterns are stress-tested with methods built to **disprove** them:

- **Edge-triggering** — one ongoing event counts once, not once per poll (kills duplication artifacts).
- **Shuffle test** — observed clusters are compared against time-randomized data (circular and schedule-aware nulls).
- **Out-of-sample backtest + block bootstrap** — is any predictive skill statistically real, or noise?
- **Base-rate comparison** — a 90% probability that merely matches the 90% base rate is *not* a finding.

The full analysis toolkit (`replay`, `shuffle_test`, `backtest`) lives in [`src/analyzers/offline/`](src/analyzers/offline/).

---

## 📊 Key finding (as of May 2026)

Across **5+ months** of clean, de-duplicated data covering all 9 domains:

> **No statistically significant cross-domain signal.** Anomalies in independent domains do not coincide more than chance predicts, and no condition reliably beats an event's base rate.

This null result is reported honestly. If a genuine signal ever appears, the same strict tests will surface it **credibly** — not by accident or wishful thinking.

---

## 🚫 No AI. Pure statistics.

Matrix Watcher intentionally uses **no artificial intelligence, neural networks, or language models** in its analysis. Every number is transparent and reproducible:

```
probability = occurrences / observations
skill       = P(event│condition) − P(event)
```

Validated with shuffle tests and bootstrap. No black boxes. No hallucinations. Just data.

---

## 🧱 Tech stack

- **Python 3.11+** — async sensor scheduler, event bus, threshold detector
- **FastAPI** — real-time API + PWA backend
- **Vanilla JS PWA** — installable dashboard, offline-capable
- **JSONL** storage — simple, append-only, auditable
- **systemd** — 24/7 operation with auto-restart + watchdog

## 📁 Project structure

```
src/
├── sensors/            # 9 independent data collectors
├── core/               # event bus, scheduler, types
├── analyzers/
│   ├── online/         # threshold detector, cluster detector,
│   │                   #   anomaly index, pattern tracker, digest
│   └── offline/        # replay, shuffle test, backtest (the rigor)
├── monitoring/         # health, alerting, calibration
└── storage/            # JSONL storage manager
web/                    # FastAPI API + PWA dashboard
```

## 🚀 Run it yourself

```bash
git clone https://github.com/amois3/matrixwatcher.space.git
cd matrixwatcher.space
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp config.example.json config.json     # all sources work key-free out of the box
python main.py                          # start the collector
python run_pwa.py                       # start the dashboard (http://localhost:5555)
```

---

## 🤝 Get involved

Matrix Watcher is open source because the right people make it better. It may be useful to you if you work in:

- **Data science / statistics** — rigorous null-result methodology, multi-stream correlation testing
- **Geophysics & space weather** — open, timestamped cross-domain observation data
- **Quantitative research** — a clean, honest framework for testing "is this signal real?"
- **Anyone** who values measurement over hype

If this resonates — for collaboration, research, or a serious conversation — open an issue or reach out.

**Author:** Aleksejs Moisejevs

---

## 📄 License

[MIT](LICENSE) © Aleksejs Moisejevs

<div align="center">
<sub>Built to watch honestly. <a href="https://matrixwatcher.space">matrixwatcher.space</a></sub>
</div>
