## Signals

* carbon-intensity
    Only as additional context 
* renewable-energy
    Percentage
* electricity-mix
    Main data source, most detailed.
    Absolute production values will be more useful than relative renewable percentage
    Use flowTraced=false for only local production
* total-load
    Important metric, but probably only used as additional context, if at all.
    Could be approximated by summing up electricity-mix, but also includes
    imports/exports.

## Requests

Always include header `auth-token: TOKEN`

## Carbon Intensity

Latest data, with estimates:
`https://api.electricitymaps.com/v3/carbon-intensity/latest?zone=PL&temporalGranularity=5_minutes`

without estimates:
`https://api.electricitymaps.com/v3/carbon-intensity/latest?zone=PL&temporalGranularity=5_minutes&disableEstimations=true`

Historical data
(cycle requests for each day, 2017 is earliest point, maybe constrain to less data):
`https://api.electricitymaps.com/v3/carbon-intensity/past-range?zone=PL&temporalGranularity=5_minutes&start=2017-01-01T00:00&end=2017-01-02T00:00`

In response, field `carbonIntensity` has value.
Other relevant fields: `datetime`, `isEstimated`.

## Renewable Energy

`https://api.electricitymaps.com/v3/renewable-energy/latest?zone=PL&temporalGranularity=5_minutes`

And without estimations, historical with past-range

In response, field `value` has value in percent
Other relevant fields: `datetime`, `isEstimated`.

## Electricity Mix

`https://api.electricitymaps.com/v3/electricity-mix/latest?zone=PL&temporalGranularity=5_minutes&flowTraced=false`

And without estimations, historical with past-range

In response, `mix` object in `data` (or `history`) array has values in MW.
`null`-values indicate non-existant power sources, so replace with zero?
Other relevant fields: `datetime`, `isEstimated`.

## Total Load

`https://api.electricitymaps.com/v3/total-load/latest?zone=PL&temporalGranularity=5_minutes`

And without estimations, historical with past-range

In response, field `value` has load in MW.
Other relevant fields: `datetime`, `isEstimated`.
