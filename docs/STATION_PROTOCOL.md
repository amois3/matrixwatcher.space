# Two-site environmental station protocol

This adapter is ready for two **real**, physically separated stations. No station
is connected or counted as a live source yet. `stations.example.json` keeps both
identities disabled until hardware, sites and calibrations have been recorded.

## Suggested kit for each site

- One [Raspberry Pi Zero 2 W](https://www.raspberrypi.com/products/raspberry-pi-zero-2-w/)
  with a fitted GPIO header, suitable power supply and microSD card. Its 2.4 GHz
  Wi-Fi must be reliable at the chosen site; use a different board if wired
  networking is needed.
- One I²C breakout using the [Sensirion SHT45](https://sensirion.com/products/catalog/SHT45)
  for temperature and humidity. Sensirion specifies factory calibration and
  typical accuracy of ±0.1 °C and ±1% RH.
- One I²C breakout using the [Bosch BMP390](https://www.bosch-sensortec.com/en/products/environmental-sensors/pressure-sensors/bmp390)
  for pressure. Bosch specifies typical absolute accuracy of ±0.5 hPa in its
  stated range. Record **surface** pressure and site altitude; values from
  different elevations must not be compared as though they were at sea level.
- A ventilated, rain-protected enclosure or radiation shield that keeps the
  sensors out of direct sun and away from heat sources. Keep the Pi's heat away
  from the sensor chamber. Log power interruptions and installation changes.

Buy two complete, separately powered sets only after choosing two installation
sites. The choice above is an engineering starting point, not a claim that
consumer parts are a calibrated scientific observatory. A side-by-side
comparison and site-specific calibration record are required before use.

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
