# Two-site environmental station protocol

This adapter is ready for two **real**, physically separated stations. No station
is connected or counted as a live source yet. `stations.example.json` keeps both
identities disabled until hardware, sites and calibrations have been recorded.

## Instrument and site requirements

- Put Station A and Station B in different buildings and local networks, with
  independently powered, calibrated pressure, temperature and humidity sensors.
  Record the sensor model, serial number, installation height, site surroundings,
  calibration method, date and expiry in a private commissioning record.
- Sample once per minute at a fixed UTC phase. Synchronise the clock to a
  reliable time source; include the measured offset and uncertainty in **every**
  record. The ingest boundary rejects either value above 100 ms, future times,
  and arrival more than 10 minutes after observation.
- Keep local environmental influences visible: heating, ventilation, direct
  sun, storms, movement, power and network outages. The two stations measure
  the same atmospheric domain; their concurrence is not two independent votes
  in a cross-domain cluster.
- Run a 30-day burn-in and compare with nearby public weather observations
  before freezing any event definition. Preserve missing intervals and any
  calibration changes. Do not retune thresholds after inspecting a candidate.

## Record contract

Send one JSON object per sample through a restricted SSH command that invokes
`python -m src.instruments.station_ingest --registry stations.json --logs logs`.
The SSH transport authenticates the station; do not expose this CLI as an
anonymous HTTP endpoint. A local station may buffer during a short outage,
but readings older than ten minutes need a separate archival path and cannot
be presented as live data.

```json
{
  "protocol_version": 1,
  "station_id": "station_a",
  "sequence": 1,
  "observed_at": 1790208060,
  "ntp_offset_ms": 5.0,
  "clock_uncertainty_ms": 10.0,
  "calibration_id": "actual-calibration-record-id",
  "temperature_celsius": 17.2,
  "humidity_percent": 48.0,
  "pressure_hpa": 1012.4
}
```

The local ingest validates the commissioned station ID, calibration expiry,
units/ranges, UTC clock quality, sequence and reporting delay. Accepted samples
are written as append-only JSONL under `logs/stations/<id>/<UTC date>.jsonl`.
They are **context only**; the anomaly bus and public source count remain
unchanged until real hardware is installed and its error characteristics are
measured. A station outage is a missing observation, never a zero reading.
