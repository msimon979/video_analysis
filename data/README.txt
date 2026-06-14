Place input CSV files here.

Expected columns (in order):
event_id, event_type, session_id, platform, content_id, timestamp, duration_ms, device_id, firmware_version, error_code

Run the pipeline against a file:
  make run INPUT=data/your_file.csv
