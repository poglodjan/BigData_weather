# Open-Meteo

## Forecast values

Base URL:
`https://api.open-meteo.com/v1/forecast?latitude=52.2298&longitude=21.0118`

### Hourly

Full request URL:
```
https://api.open-meteo.com/v1/forecast?latitude=52.2298&longitude=21.0118&forecast_days=16&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,visibility,wind_speed_10m,wind_speed_180m,wind_speed_80m,wind_speed_120m,wind_direction_10m,wind_direction_80m,wind_direction_120m,wind_direction_180m,wind_gusts_10m,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,terrestrial_radiation,sunshine_duration
```

Hourly forecast time range:
`&forecast_days=16`

Hourly values (append to base):
`&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,visibility,wind_speed_10m,wind_speed_180m,wind_speed_80m,wind_speed_120m,wind_direction_10m,wind_direction_80m,wind_direction_120m,wind_direction_180m,wind_gusts_10m,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,terrestrial_radiation,sunshine_duration`

```
temperature_2m
relative_humidity_2m
dew_point_2m
apparent_temperature
precipitation_probability
precipitation
rain
showers
snowfall
snow_depth
pressure_msl
surface_pressure
cloud_cover
cloud_cover_low
cloud_cover_mid
cloud_cover_high
visibility
wind_speed_10m
wind_speed_180m
wind_speed_80m
wind_speed_120m
wind_direction_10m
wind_direction_80m
wind_direction_120m
wind_direction_180m
wind_gusts_10m
shortwave_radiation
direct_radiation
diffuse_radiation
direct_normal_irradiance
terrestrial_radiation
sunshine_duration
```


### 15-Minutely

15-Minutely time range (maximally 24 hours forecast):
`&forecast_minutely_15=96`

15-Minutely values (append to base):

`&minutely_15=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,direct_radiation,shortwave_radiation,diffuse_radiation,direct_normal_irradiance,terrestrial_radiation,rain,snowfall,sunshine_duration,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,wind_gusts_10m,visibility&start_date=2025-11-12&end_date=2025-11-26&forecast_minutely_15=96`

```
temperature_2m
relative_humidity_2m
dew_point_2m
apparent_temperature
precipitation
direct_radiation
shortwave_radiation
diffuse_radiation
direct_normal_irradiance
terrestrial_radiation
rain
snowfall
sunshine_duration
wind_speed_10m
wind_speed_80m
wind_direction_10m
wind_direction_80m
wind_gusts_10m
visibility
```


### Current

Take hourly command with s/hourly/current/

Full request URL:
```
https://api.open-meteo.com/v1/forecast?latitude=52.2298&longitude=21.0118&forecast_days=16&current=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,visibility,wind_speed_10m,wind_speed_180m,wind_speed_80m,wind_speed_120m,wind_direction_10m,wind_direction_80m,wind_direction_120m,wind_direction_180m,wind_gusts_10m,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,terrestrial_radiation,sunshine_duration
```

`interval` value in response indicates timespan of integrated parameters,
expected to be 900 seconds (15 minutes).
Should also be updated every 15 minutes.


### Historical

`https://archive-api.open-meteo.com/v1/archive?latitude=52.2298&longitude=21.0118&start_date=2025-01-01&end_date=2025-01-02`

```
https://archive-api.open-meteo.com/v1/archive?latitude=52.2298&longitude=21.0118&start_date=2025-01-01&end_date=2025-01-02&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,visibility,wind_speed_10m,wind_speed_180m,wind_speed_80m,wind_speed_120m,wind_direction_10m,wind_direction_80m,wind_direction_120m,wind_direction_180m,wind_gusts_10m,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,terrestrial_radiation,sunshine_duration


```

With appropriate start and end dates, and append list of hourly parameters.

Maybe pull in yearly increments, take [rate limits](https://open-meteo.com/en/pricing) into account.

Data earlier than 2017 won't be of much use, since electricitymaps historical
data starts at 2017.
