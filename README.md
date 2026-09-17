# us-power-weather-relay

Hourly ISO market-data relay. A GitHub Actions US-hosted runner
(`.github/workflows/iso-relay.yml`) pulls public ISO data that is
geo-blocked from the main project's deploy host and commits rolling
90-day CSVs to `data/relay/`.

Consumed by the [us-power-weather] tail-risk radar via
`raw.githubusercontent.com` (`USP_RELAY_BASE`).

## Files

| CSV | Content | Granularity |
|---|---|---|
| `ercot_da.csv` | ERCOT DAM settlement-point prices, hub mean | hourly |
| `ercot_rt.csv` | ERCOT RTM SPP, hub mean | 15-min |
| `ercot_ordc.csv` | ORDC price adders + online reserves | SCED tick |
| `ercot_as.csv` | ERCOT ancillary-service prices | per report |
| `spp_da.csv` | SPP day-ahead hub LMP mean | hourly |
| `pjm_da.csv` | PJM day-ahead hub/zone LMP mean (needs `PJM_API_KEY` secret) | hourly |

Data is public ISO report data; ERCOT ToU permits use/redistribution of
public data in compilations and analyses.
