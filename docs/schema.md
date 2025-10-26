# Flight Delays — Canonical Schema (v1)

**Domain:** flight_delays  
**Grain:** one row per flight leg (unique by flight_date + origin + dest + airline_code + flight_number + scheduled_dep)

## Columns & Types

| column_name        | type        | nullable | description                                                                 | examples                          | notes/rules                                                                                   |
|---------------------|-------------|-----------|-----------------------------------------------------------------------------|-----------------------------------|------------------------------------------------------------------------------------------------|
| flight_date         | date        | false     | Local calendar date of departure (YYYY-MM-DD).                              | 2025-10-19                        | Must be a valid date.                                                                         |
| airline_code        | string      | false     | Marketing carrier IATA code (2-char).                                       | AA, DL, UA, BA                    | Uppercase A–Z; length 2–3 (IATA/ICAO tolerant).                                               |
| airline_name        | string      | true      | Carrier name.                                                               | American Airlines                 | Optional convenience field.                                                                   |
| flight_number       | string      | false     | Flight number string (leading zeros allowed).                               | 007, 2314                         | Treat as string to preserve formatting.                                                       |
| tail_number         | string      | true      | Aircraft registration if known.                                             | N123AA, G-ABCD                    | Optional.                                                                                     |
| origin              | string      | false     | Origin airport IATA code (3-char).                                          | JFK, LHR, CDG                     | Uppercase A–Z; length = 3.                                                                    |
| origin_city         | string      | true      | Origin city name.                                                           | New York, London                  | Optional.                                                                                     |
| dest                | string      | false     | Destination airport IATA code (3-char).                                     | LAX, AMS, FRA                     | Uppercase A–Z; length = 3.                                                                    |
| dest_city           | string      | true      | Destination city name.                                                      | Los Angeles, Amsterdam            | Optional.                                                                                     |
| scheduled_dep       | timestamp   | false     | Scheduled departure local timestamp (ISO 8601).                             | 2025-10-19T14:30:00               | Local to origin airport; no timezone suffix.                                                  |
| dep_time            | timestamp   | true      | Actual departure local timestamp (ISO 8601).                                | 2025-10-19T14:52:00               | May be null if cancelled.                                                                     |
| dep_delay_minutes   | integer     | true      | Departure delay in whole minutes (actual - scheduled).                      | 22, 0, -3                         | Negative allowed for early depart; null if cancelled.                                         |
| scheduled_arr       | timestamp   | false     | Scheduled arrival local timestamp (ISO 8601).                               | 2025-10-19T17:40:00               | Local to destination airport; no timezone suffix.                                             |
| arr_time            | timestamp   | true      | Actual arrival local timestamp (ISO 8601).                                  | 2025-10-19T17:55:00               | Null if cancelled/diverted.                                                                   |
| arr_delay_minutes   | integer     | true      | Arrival delay in whole minutes (actual - scheduled).                        | 15, 0, -5                         | Should align with timestamps when both present.                                               |
| cancelled           | boolean     | false     | Whether the flight leg was cancelled.                                       | true, false                       | If true → dep_time, arr_time, delays may be null.                                             |
| cancellation_code   | string      | true      | Reason code when cancelled.                                                 | WX, MX, ATC, NA                   | Enum when present; null if not cancelled.                                                     |
| diverted            | boolean     | false     | Whether the flight leg diverted.                                            | true, false                       | If true → dest may reflect planned dest; arr_time may be for diversion station (see note).    |
| wheels_off          | timestamp   | true      | Local timestamp when wheels left ground.                                    | 2025-10-19T15:02:00               | Optional ops metric.                                                                          |
| wheels_on           | timestamp   | true      | Local timestamp when wheels touched down.                                   | 2025-10-19T17:43:00               | Optional ops metric.                                                                          |
| taxi_out_minutes    | integer     | true      | Taxi-out duration in minutes.                                               | 12                                | Non-negative when present.                                                                    |
| taxi_in_minutes     | integer     | true      | Taxi-in duration in minutes.                                                | 5                                 | Non-negative when present.                                                                    |
| air_time_minutes    | integer     | true      | Airborne time in minutes.                                                   | 161                               | Non-negative when present.                                                                    |
| distance_miles      | integer     | true      | Great-circle distance miles (approx).                                       | 2475                              | Positive when present.                                                                        |
| equipment           | string      | true      | Equipment/aircraft type code.                                               | A320, B738                        | Optional.                                                                                     |
| data_source         | string      | false     | Provenance tag for audits.                                                  | seed_sample_v1                    | e.g., vendor batch, manual seed.                                                              |
| ingest_ts           | timestamp   | false     | Load-time timestamp (UTC) assigned at ingest.                               | 2025-10-20T12:34:56Z              | Populated by ingest script if not present.                                                    |

## Primary Key (Proposed)
`flight_date, origin, dest, airline_code, flight_number, scheduled_dep`

## Partitioning (Processed Zone — Forward Plan)
`year, month, day, origin` derived from `flight_date` and `origin`.

## Validation Hints (for GE later)
- Non-null: flight_date, airline_code, flight_number, origin, dest, scheduled_dep, scheduled_arr, cancelled, diverted, data_source  
- Code formats: IATA codes are 2–3 (airline) / 3 (airport) uppercase A–Z.  
- Timestamps: ISO 8601 without TZ suffix for local times; `ingest_ts` uses UTC with `Z`.  
- Delay integrity: when dep_time and scheduled_dep are present → dep_delay_minutes = round((dep_time − scheduled_dep)/60s). Similar for arrival.  
- Logical: if cancelled = true → dep_time, arr_time, delays can be null; cancellation_code should be present or NA.  
