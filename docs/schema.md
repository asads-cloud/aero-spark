# Flight Delays — Canonical Schema (v1)

**Domain:** flight_delays  
**Grain:** one row per flight leg (unique by flight_date + origin + dest + airline_code + flight_number + scheduled_dep)

## Columns & Types

| # | Column               | Type          | Nullable | Description                                                                 | Examples                          | Notes/Rules                                                                                   |
|---|----------------------|---------------|----------|-----------------------------------------------------------------------------|-----------------------------------|------------------------------------------------------------------------------------------------|
| 1 | flight_date          | date          | NO       | Local calendar date of departure (YYYY-MM-DD).                              | 2025-10-19                        | Must be a valid date.                                                                         |
| 2 | airline_code         | string        | NO       | Marketing carrier IATA code (2-char).                                       | AA, DL, UA, BA                    | Uppercase A–Z; length 2–3 (IATA/ICAO tolerant).                                               |
| 3 | airline_name         | string        | YES      | Carrier name.                                                                | American Airlines                 | Optional convenience field.                                                                   |
| 4 | flight_number        | string        | NO       | Flight number string (leading zeros allowed).                                | 007, 2314                         | Treat as string to preserve formatting.                                                       |
| 5 | tail_number          | string        | YES      | Aircraft registration if known.                                              | N123AA, G-ABCD                    | Optional.                                                                                     |
| 6 | origin               | string        | NO       | Origin airport IATA code (3-char).                                           | JFK, LHR, CDG                     | Uppercase A–Z; length = 3.                                                                    |
| 7 | origin_city          | string        | YES      | Origin city name.                                                            | New York, London                  | Optional.                                                                                     |
| 8 | dest                 | string        | NO       | Destination airport IATA code (3-char).                                      | LAX, AMS, FRA                     | Uppercase A–Z; length = 3.                                                                    |
| 9 | dest_city            | string        | YES      | Destination city name.                                                       | Los Angeles, Amsterdam            | Optional.                                                                                     |
|10 | scheduled_dep        | timestamp     | NO       | Scheduled departure local timestamp (ISO 8601).                              | 2025-10-19T14:30:00               | Local to origin airport; no timezone suffix.                                                  |
|11 | dep_time             | timestamp     | YES      | Actual departure local timestamp (ISO 8601).                                 | 2025-10-19T14:52:00               | May be null if cancelled.                                                                     |
|12 | dep_delay_minutes    | integer       | YES      | Departure delay in whole minutes (actual - scheduled).                       | 22, 0, -3                         | Negative allowed for early depart; null if cancelled.                                         |
|13 | scheduled_arr        | timestamp     | NO       | Scheduled arrival local timestamp (ISO 8601).                                | 2025-10-19T17:40:00               | Local to destination airport; no timezone suffix.                                            |
|14 | arr_time             | timestamp     | YES      | Actual arrival local timestamp (ISO 8601).                                   | 2025-10-19T17:55:00               | Null if cancelled/diverted.                                                                   |
|15 | arr_delay_minutes    | integer       | YES      | Arrival delay in whole minutes (actual - scheduled).                         | 15, 0, -5                         | Should align with timestamps when both present.                                               |
|16 | cancelled            | boolean       | NO       | Whether the flight leg was cancelled.                                        | true, false                       | If true → dep_time, arr_time, delays may be null.                                             |
|17 | cancellation_code    | string        | YES      | Reason code when cancelled.                                                  | WX, MX, ATC, NA                   | Enum when present; null if not cancelled.                                                     |
|18 | diverted             | boolean       | NO       | Whether the flight leg diverted.                                             | true, false                       | If true → dest may reflect planned dest; arr_time may be for diversion station (see note).    |
|19 | wheels_off           | timestamp     | YES      | Local timestamp when wheels left ground.                                     | 2025-10-19T15:02:00               | Optional ops metric.                                                                          |
|20 | wheels_on            | timestamp     | YES      | Local timestamp when wheels touched down.                                    | 2025-10-19T17:43:00               | Optional ops metric.                                                                          |
|21 | taxi_out_minutes     | integer       | YES      | Taxi-out duration in minutes.                                                | 12                                | Non-negative when present.                                                                    |
|22 | taxi_in_minutes      | integer       | YES      | Taxi-in duration in minutes.                                                 | 5                                 | Non-negative when present.                                                                    |
|23 | air_time_minutes     | integer       | YES      | Airborne time in minutes.                                                    | 161                               | Non-negative when present.                                                                    |
|24 | distance_miles       | integer       | YES      | Great-circle distance miles (approx).                                        | 2475                              | Positive when present.                                                                        |
|25 | equipment            | string        | YES      | Equipment/aircraft type code.                                                | A320, B738                        | Optional.                                                                                     |
|26 | data_source          | string        | NO       | Provenance tag for audits.                                                   | seed_sample_v1                    | e.g., vendor batch, manual seed.                                                              |
|27 | ingest_ts            | timestamp     | NO       | Load-time timestamp (UTC) assigned at ingest.                                | 2025-10-20T12:34:56Z              | Populated by ingest script if not present.                                                    |

## Primary Key (Proposed)
`flight_date, origin, dest, airline_code, flight_number, scheduled_dep`

## Partitioning (Processed Zone — Forward Plan)
`year, month, day, origin` derived from `flight_date` and `origin`.

## Validation Hints (for GE later)
- Non-null: flight_date, airline_code, flight_number, origin, dest, scheduled_dep, scheduled_arr, cancelled, diverted, data_source
- Code formats: IATA codes are 2–3 (airline) / 3 (airport) uppercase A–Z.
- Timestamps: ISO 8601 without TZ suffix for local times; `ingest_ts` uses UTC with `Z`.
- Delay integrity: when dep_time and scheduled_dep are present → dep_delay_minutes = round((dep_time - scheduled_dep)/60s). Similar for arrival.
- Logical: if cancelled=true → dep_time, arr_time, delays can be null; cancellation_code should be present or NA.