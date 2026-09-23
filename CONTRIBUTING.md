# Contributing to Matrix Watcher

Matrix Watcher is an open observation experiment. Changes to collection, detection and presentation should make both findings and measurement gaps easier to inspect.

## Start here

1. Check [existing issues](https://github.com/amois3/matrixwatcher.space/issues) and describe the problem or proposed feature, including the affected source and a way to reproduce it.
2. Fork [this repository](https://github.com/amois3/matrixwatcher.space), make a focused branch, and open a pull request with the behaviour changed and verification performed.
3. For a new source, state whether it is eligible for anomaly detection, a calculated covariate or context only. Sources from one physical process or upstream API must not be counted as independent domains.

```bash
git clone https://github.com/YOUR_USERNAME/matrixwatcher.space.git
cd matrixwatcher.space
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.json config.json
python -m pytest tests -q
```

The dashboard runs with `python run_pwa.py`; the collector runs with `python main.py`. Review the [README](README.md) and `/api/coverage` before interpreting a missing result.

## Research changes

- Preserve the source observation timestamp, retrieval time, units, station or feed identity, and quality status. Mark missing, stale or partial observations explicitly.
- Keep raw records auditable. Historical exploratory analysis must remain labelled exploratory; code fixes cannot retroactively fill collection gaps.
- For an anomaly or forecast claim, define the event and comparison in advance, group repeated readings into episodes, use an appropriate matched control, account for all comparisons tried, and validate on later data.
- Keep context sources outside anomaly clusters unless there is a documented hypothesis and a separate validation plan. A weather model grid and an airport report are different measurements.

Add tests for behaviour that could silently alter data quality, event counts, timestamps, domain grouping or conclusions. The live service is operated separately from a contributor's local checkout; a pull request should not contain production logs or secrets.
