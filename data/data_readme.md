# Formula 1 Data Project

## Overview
This project utilizes real-time and historical Formula 1 data from the OpenF1 API. The data is stored in an SQLite database and includes information on cars, drivers, intervals, laps, locations, meetings, pit stops, positions, race control, sessions, stints, team radio communications, and weather.

## Data Features

### Car Data
- **Attributes**: brake, date, driver_number, drs, meeting_key, n_gear, rpm, session_key, speed, throttle
- **Description**: Telemetry data of cars at a sample rate of about 3.7 Hz.

### Drivers
- **Attributes**: broadcast_name, country_code, driver_number, first_name, full_name, headshot_url, last_name, meeting_key, name_acronym, session_key, team_colour, team_name
- **Description**: Information about drivers for each session.

### Intervals
- **Attributes**: date, driver_number, gap_to_leader, interval, meeting_key, session_key
- **Description**: Real-time interval data between drivers and their gap to the race leader.

### Laps
- **Attributes**: date_start, driver_number, duration_sector_1, duration_sector_2, duration_sector_3, i1_speed, i2_speed, is_pit_out_lap, lap_duration, lap_number, meeting_key, segments_sector_1, segments_sector_2, segments_sector_3, session_key, st_speed
- **Description**: Detailed information about individual laps.

### Location
- **Attributes**: date, driver_number, meeting_key, session_key, x, y, z
- **Description**: Approximate location of cars on the circuit.

### Meetings
- **Attributes**: circuit_key, circuit_short_name, country_code, country_key, country_name, date_start, gmt_offset, location, meeting_key, meeting_name, meeting_official_name, year
- **Description**: Information about Grand Prix or testing weekends.

### Pit
- **Attributes**: date, driver_number, lap_number, meeting_key, pit_duration, session_key
- **Description**: Data about cars going through the pit lane.

### Position
- **Attributes**: date, driver_number, position, meeting_key, session_key
- **Description**: Positions of drivers in a race.

### Race Control
- **Attributes**: date, message, session_key, type
- **Description**: Race control messages.

### Sessions
- **Attributes**: circuit_key, circuit_short_name, country_code, country_key, country_name, date_end, date_start, gmt_offset, location, meeting_key, session_key, session_name, session_type, year
- **Description**: Information about different sessions within a meeting.

### Stints
- **Attributes**: compound, driver_number, lap_end, lap_start, meeting_key, session_key, stint_number, tyre_age_at_start
- **Description**: Information about stints of drivers during a session.

### Team Radio
- **Attributes**: date, driver_number, message, session_key, recording_url
- **Description**: Team radio communications.

### Weather
- **Attributes**: date, air_temperature, track_temperature, humidity, pressure, rainfall, session_key, meeting_key, wind_direction, wind_speed
- **Description**: Weather conditions during a session.

## Data Source
All data is fetched from the [OpenF1 API](https://openf1.org/).
