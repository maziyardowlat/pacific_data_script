#!/usr/bin/env python3
"""
transform_to_datastream.py

Transforms raw NHG stream temperature logger CSVs into DataStream upload format.
One output CSV (+ a zipped copy) is created per station code found in the input data.

Folder layout (all paths relative to wherever this script lives):
    transform_to_datastream.py                <-- this script
    NHG_STloggers_mapping_updated.xlsx        <-- mapping file (same folder)
    NHG_Station_Summary_Stephen.csv           <-- station summary with logger cutoffs
    input/                                    <-- drop raw compiled CSVs here
        01FW001_compiled_2020.csv
        ...
    output/                                   <-- created automatically
        01FW001_datastream.csv
        01FW001_datastream.zip
        ...

Usage:
    python transform_to_datastream.py

    Optionally override the dataset name:
        python transform_to_datastream.py --dataset_name "My Custom Name"

Dependencies:
    pip install pandas openpyxl
"""

import argparse
import sys
import zipfile
import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION — edit these if your setup changes
# =============================================================================

# Default DatasetName embedded in every row.
# Must match exactly what you enter in the DataStream metadata form.
DEFAULT_DATASET_NAME = (
    "Sub-hourly Water Temperature Data Collected by UNBC's "
    "Northern Hydrometeorology Group (NHG) across northern BC"
)

# UTC offset applied when converting raw timestamps.
# Raw data is in UTC; we convert to Pacific (UTC-7) for DataStream output.
# Using a fixed -07:00 offset (no DST adjustment).
UTC_OFFSET_HOURS = -7
ACTIVITY_START_TIMEZONE = "-07:00"

# Names of the mapping CSV files (expected in the same folder as this script)
STATIONS_MAPPING_FILENAME = "NHG_STloggers_mapping_full 1_SJD(stations).csv"
RESULT_STATUS_MAPPING_FILENAME = "NHG_STloggers_mapping_full 1_SJD(result_status).csv"

# Name of the station summary CSV with MX2201→MX2203 cutoff timestamps.
# Expected columns: Station, MX2201 Last Measurement
# The cutoff timestamps are in UTC.
STATION_SUMMARY_FILENAME = "NHG_Station_Summary_Stephen.csv"

# Subfolder names relative to the script's location
INPUT_FOLDER  = "input"
OUTPUT_FOLDER = "output"

# Raw input columns to drop — not part of the DataStream schema
COLS_TO_DROP = ["data_id", "utc_offset"]

# DataStream columns in the order the upload template expects them
DATASTREAM_COLUMN_ORDER = [
    "DatasetName",
    "MonitoringLocationID",
    "MonitoringLocationName",
    "MonitoringLocationLatitude",
    "MonitoringLocationLongitude",
    "MonitoringLocationHorizontalCoordinateReferenceSystem",
    "MonitoringLocationType",
    "ActivityType",
    "ActivityMediaName",
    "ActivityStartDate",
    "ActivityStartTime",
    "ActivityStartTimeZone",
    "CharacteristicName",
    "ResultValue",
    "ResultUnit",
    "ResultValueType",
    "ResultStatusID",
    "ResultComment",
    "ResultDetectionCondition",
    "ResultAnalyticalMethodContext",
    "ResultAnalyticalMethodID",
    "ResultAnalyticalMethodName",
    "SampleCollectionEquipmentName",
]


# =============================================================================
# STEP 1 — Load the mapping file and station summary
# =============================================================================

def load_mapping(stations_path: Path, result_status_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load the stations and result_status mapping CSVs.

    Returns:
        stations_df      — one row per station, keyed by station_code
        result_status_df — one row per flag value, keyed by wtmp_flag
    """
    stations_df = pd.read_csv(stations_path, dtype=str)
    result_status_df = pd.read_csv(result_status_path, dtype=str)

    # Drop any completely empty rows left over from formatting
    stations_df = stations_df.dropna(how="all")
    result_status_df = result_status_df.dropna(subset=["wtmp_flag"])

    return stations_df, result_status_df


def load_station_summary(summary_path: Path) -> dict[str, pd.Timestamp | None]:
    """
    Load the station summary CSV and build a lookup dict mapping
    station codes to their MX2201→MX2203 cutoff timestamp (UTC).

    Stations with 'N/A' or missing cutoff dates get None, meaning
    all their data is treated as one logger model (see notes column).

    Returns:
        dict of { station_code: cutoff_timestamp_utc_or_None }
    """
    summary_df = pd.read_csv(summary_path)
    cutoffs: dict[str, pd.Timestamp | None] = {}

    for _, row in summary_df.iterrows():
        station = row["Station"]
        raw_cutoff = row.get("MX2201 Last Measurement", "N/A")

        # Parse the cutoff; treat "N/A" or blank as no cutoff
        if pd.isna(raw_cutoff) or str(raw_cutoff).strip().upper() == "N/A":
            cutoffs[station] = None
        else:
            cutoffs[station] = pd.to_datetime(str(raw_cutoff).strip())

    return cutoffs


# =============================================================================
# STEP 2 — Parse timestamps (UTC → UTC-7 conversion)
# =============================================================================

def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the 'timestamp' column from UTC to local time (UTC-7),
    then split into separate DataStream fields:
        ActivityStartDate      — YYYY-MM-DD  (in local time)
        ActivityStartTime      — HH:MM:SS    (in local time)
        ActivityStartTimeZone  — "-07:00"

    Also stores the original UTC timestamp in a temporary column
    '_timestamp_utc' for use in downstream steps (e.g. logger model
    cutoff comparison). This column is dropped later.

    The original 'timestamp' column is dropped after splitting.
    """
    utc_ts = pd.to_datetime(df["timestamp"])

    # Keep the original UTC timestamp for later comparisons
    df["_timestamp_utc"] = utc_ts

    # Convert UTC → local (UTC-7)
    local_ts = utc_ts + pd.Timedelta(hours=UTC_OFFSET_HOURS)

    df["ActivityStartDate"]     = local_ts.dt.strftime("%Y-%m-%d")
    df["ActivityStartTime"]     = local_ts.dt.strftime("%H:%M:%S")
    df["ActivityStartTimeZone"] = ACTIVITY_START_TIMEZONE

    df = df.drop(columns=["timestamp"])
    return df


# =============================================================================
# STEP 3 — Map quality flags
# =============================================================================

def map_flags(df: pd.DataFrame, result_status_df: pd.DataFrame) -> pd.DataFrame:
    """
    Translate the raw 'wtmp_flag' column into three DataStream fields:
        ResultComment          — human-readable flag description (Pass, Missing, etc.)
        ResultStatusID         — validation status (Validated)
        ResultDetectionCondition — "Unable to Measure" when no valid measurement exists

    Enforces DataStream schema rules (per Nell's feedback):
        - If ResultValue is NA → ResultDetectionCondition must be filled,
          and ResultUnit must also be NA.
        - If ResultDetectionCondition is empty → ResultValue and ResultUnit
          must contain real values.

    This applies to two cases:
        1. wtmp_flag is NaN (no flag → no measurement taken)
        2. wtmp_flag exists but ResultValue is NaN/empty (e.g. "Missing" flag)

    The original 'wtmp_flag' column is dropped after mapping.
    """
    # Build lookup dicts from the mapping sheet
    flag_to_comment = dict(zip(result_status_df["wtmp_flag"], result_status_df["ResultComment"]))
    flag_to_status  = dict(zip(result_status_df["wtmp_flag"], result_status_df["ResultStatusID"]))

    df["ResultComment"]            = df["wtmp_flag"].map(flag_to_comment)
    df["ResultStatusID"]           = df["wtmp_flag"].map(flag_to_status)
    df["ResultDetectionCondition"] = ""

    # Cast ResultValue to object first so we can mix floats and strings
    df["ResultValue"] = df["ResultValue"].astype(object)

    # Identify rows with no valid measurement:
    #   1. wtmp_flag is NaN (no flag at all → no measurement taken)
    #   2. ResultValue is NaN/empty (value missing, even if a flag exists)
    no_flag_mask  = df["wtmp_flag"].isna()
    no_value_mask = df["ResultValue"].isna() | (df["ResultValue"] == "")
    missing_mask  = no_flag_mask | no_value_mask

    # Per DataStream schema (from Nell):
    #   If ResultValue is NA → ResultDetectionCondition MUST be filled,
    #                           and ResultUnit MUST also be NA.
    #   If ResultDetectionCondition is empty → ResultValue and ResultUnit
    #                                          MUST have real values.
    df.loc[missing_mask, "ResultValue"]               = "NA"
    df.loc[missing_mask, "ResultUnit"]                = "NA"
    df.loc[missing_mask, "ResultDetectionCondition"]  = "Unable to Measure"

    # Per metadata: "All observations carry a ResultStatusID of 'Validated',
    # with the exception of records where no measurement was possible
    # (ResultDetectionCondition: Unable to Measure)."
    # So clear ResultStatusID for ALL missing-value rows, not just no-flag ones.
    df.loc[missing_mask, "ResultStatusID"] = ""

    df = df.drop(columns=["wtmp_flag"])

    # Replace any remaining NaN in text columns with empty strings
    for col in ["ResultComment", "ResultStatusID", "ResultDetectionCondition"]:
        df[col] = df[col].fillna("")

    return df


# =============================================================================
# STEP 3.5 — Assign logger model (MX2201 vs MX2203) based on cutoff
# =============================================================================

def assign_logger_model(
    df: pd.DataFrame,
    cutoff_utc: pd.Timestamp | None,
) -> pd.DataFrame:
    """
    Set ResultAnalyticalMethodID to the correct logger model based on
    whether each row's timestamp falls before or after the MX2201→MX2203
    cutoff from the station summary.

    Uses the '_timestamp_utc' column (original UTC timestamps) for
    comparison, since the cutoff timestamps are also in UTC.

    Rules:
        - timestamp <= cutoff  → MX2201
        - timestamp >  cutoff  → MX2203
        - No cutoff (None)     → leave as-is (whatever the mapping set)
    """
    if cutoff_utc is None:
        # No cutoff info for this station — skip (keeps mapping default)
        return df

    is_mx2201 = df["_timestamp_utc"] <= cutoff_utc

    df.loc[is_mx2201,  "ResultAnalyticalMethodID"] = "MX2201"
    df.loc[~is_mx2201, "ResultAnalyticalMethodID"] = "MX2203"

    mx2201_count = is_mx2201.sum()
    mx2203_count = (~is_mx2201).sum()
    print(f"    Logger split: {mx2201_count:,} MX2201 | {mx2203_count:,} MX2203")

    return df


# =============================================================================
# STEP 4 — Attach station metadata
# =============================================================================

def attach_station_metadata(df: pd.DataFrame, station_row: pd.Series) -> pd.DataFrame:
    """
    Broadcast all station-level metadata columns from the mapping file
    across every row in the dataframe (these values are constant per station).

    Only columns present in DATASTREAM_COLUMN_ORDER are pulled from the
    mapping row, so extra mapping columns are safely ignored.
    """
    # Columns sourced from the stations mapping sheet
    STATION_META_COLS = [
        "MonitoringLocationID",
        "MonitoringLocationName",
        "MonitoringLocationLatitude",
        "MonitoringLocationLongitude",
        "MonitoringLocationHorizontalCoordinateReferenceSystem",
        "MonitoringLocationType",
        "ActivityType",
        "ActivityMediaName",
        "CharacteristicName",
        "ResultUnit",
        "ResultValueType",
        "ResultAnalyticalMethodContext",
        "ResultAnalyticalMethodID",
        "ResultAnalyticalMethodName",
        "SampleCollectionEquipmentName",
    ]

    for col in STATION_META_COLS:
        # Use empty string for any missing optional fields rather than NaN,
        # to keep the output CSV clean
        value = station_row.get(col, "")
        value = "" if pd.isna(value) else value

        if col in df.columns:
            # Preserve values already set by earlier steps (e.g. ResultUnit
            # set to "NA" by map_flags for missing observations)
            df[col] = df[col].fillna(value)
            df.loc[df[col] == "", col] = value
        else:
            df[col] = value

    return df


# =============================================================================
# STEP 5 — Assemble final column order
# =============================================================================

def assemble_output(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Add the DatasetName column and reorder all columns to match the
    DataStream upload template. Any columns not in DATASTREAM_COLUMN_ORDER
    are silently dropped (including temporary columns like _timestamp_utc).
    """
    df["DatasetName"] = dataset_name

    # Only keep columns that are both expected and present in our df
    cols_to_write = [c for c in DATASTREAM_COLUMN_ORDER if c in df.columns]
    return df[cols_to_write]


# =============================================================================
# Core per-file processing
# =============================================================================

def process_csv(
    csv_path: Path,
    stations_df: pd.DataFrame,
    result_status_df: pd.DataFrame,
    dataset_name: str,
    logger_cutoffs: dict[str, pd.Timestamp | None],
) -> tuple[str, pd.DataFrame]:
    """
    Run all transformation steps on a single raw CSV file.

    Returns:
        station_code  — the station identifier found in this file
        output_df     — DataStream-formatted DataFrame ready to write to CSV
    """
    df = pd.read_csv(csv_path)

    # --- Identify the station ---
    station_codes = df["station_code"].unique()
    if len(station_codes) > 1:
        raise ValueError(
            f"Expected one station per file, found multiple: {station_codes}. "
            f"Please split this file before running."
        )
    station_code = station_codes[0]

    # Look up the station's metadata row from the mapping file
    station_rows = stations_df[stations_df["station_code"] == station_code]
    if station_rows.empty:
        raise ValueError(
            f"Station '{station_code}' not found in the mapping file. "
            f"Please add it to the 'stations' sheet and re-run."
        )
    station_row = station_rows.iloc[0]

    # Look up the MX2201→MX2203 cutoff for this station
    cutoff_utc = logger_cutoffs.get(station_code)

    print(f"  Station: {station_code}  |  Rows: {len(df):,}")
    if cutoff_utc is not None:
        print(f"    MX2201 last measurement (UTC): {cutoff_utc}")
    else:
        print(f"    No MX2201 cutoff — using default logger from mapping")

    # --- Rename wtmp → ResultValue before further processing ---
    df = df.rename(columns={"wtmp": "ResultValue"})

    # --- Run transformation steps ---
    df = df.drop(columns=COLS_TO_DROP, errors="ignore")    # drop unused columns
    df = parse_timestamps(df)                               # UTC → UTC-7 + split
    df = map_flags(df, result_status_df)                    # expand flag column
    df = attach_station_metadata(df, station_row)           # add station info
    df = assign_logger_model(df, cutoff_utc)                # MX2201 vs MX2203
    df = df.drop(columns=["station_code", "logger_serial", "_timestamp_utc"],
                 errors="ignore")
    df = assemble_output(df, dataset_name)                  # final column order

    return station_code, df


# =============================================================================
# Zip helper
# =============================================================================

def zip_csv(csv_path: Path) -> Path:
    """
    Create a .zip archive containing a single CSV file.
    The zip is saved alongside the CSV with the same stem.

    Returns:
        zip_path — Path to the newly created .zip file
    """
    zip_path = csv_path.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # arcname = just the filename, no parent directories inside the zip
        zf.write(csv_path, arcname=csv_path.name)
    return zip_path


# =============================================================================
# Entry point
# =============================================================================

def main():
    # --- Resolve all paths relative to the script's own location ---
    script_dir  = Path(__file__).resolve().parent
    input_dir   = script_dir / INPUT_FOLDER
    output_dir  = script_dir / OUTPUT_FOLDER
    stations_mapping = script_dir / STATIONS_MAPPING_FILENAME
    result_status_mapping = script_dir / RESULT_STATUS_MAPPING_FILENAME
    summary     = script_dir / STATION_SUMMARY_FILENAME

    # --- Optional CLI override for dataset name ---
    parser = argparse.ArgumentParser(
        description="Transform NHG logger CSVs into DataStream upload format.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dataset_name",
        default=DEFAULT_DATASET_NAME,
        help=(
            "DatasetName value embedded in every row. "
            "Must match exactly what you enter in the DataStream metadata form. "
            f"Defaults to: '{DEFAULT_DATASET_NAME}'"
        ),
    )
    args = parser.parse_args()

    # --- Validate inputs before doing any work ---
    if not input_dir.is_dir():
        sys.exit(
            f"Error: Expected an '{INPUT_FOLDER}/' folder next to this script.\n"
            f"       Looked for: {input_dir}\n"
            f"       Create it and drop your raw CSVs inside."
        )
    if not stations_mapping.is_file():
        sys.exit(
            f"Error: Stations mapping file not found next to this script.\n"
            f"       Looked for: {stations_mapping}\n"
            f"       Make sure '{STATIONS_MAPPING_FILENAME}' is in the same folder."
        )
    if not result_status_mapping.is_file():
        sys.exit(
            f"Error: Result status mapping file not found next to this script.\n"
            f"       Looked for: {result_status_mapping}\n"
            f"       Make sure '{RESULT_STATUS_MAPPING_FILENAME}' is in the same folder."
        )
    if not summary.is_file():
        sys.exit(
            f"Error: Station summary not found next to this script.\n"
            f"       Looked for: {summary}\n"
            f"       Make sure '{STATION_SUMMARY_FILENAME}' is in the same folder."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Load mapping tables and station summary ---
    print("Loading mapping file...")
    stations_df, result_status_df = load_mapping(stations_mapping, result_status_mapping)
    print(f"  Stations loaded:      {len(stations_df)}")
    print(f"  Flag mappings loaded: {len(result_status_df)}")

    print("Loading station summary...")
    logger_cutoffs = load_station_summary(summary)
    cutoffs_with_dates = sum(1 for v in logger_cutoffs.values() if v is not None)
    print(f"  Stations with MX2201 cutoffs: {cutoffs_with_dates}")

    # --- Find CSV files in the input folder ---
    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        sys.exit(f"Error: No CSV files found in '{input_dir}'.")
    print(f"\nFound {len(csv_files)} CSV file(s) to process:\n")

    # --- Process each file, grouping results by station ---
    station_results: dict[str, list[pd.DataFrame]] = {}

    for csv_path in csv_files:
        print(f"[{csv_path.name}]")
        try:
            station_code, df = process_csv(
                csv_path, stations_df, result_status_df,
                args.dataset_name, logger_cutoffs,
            )
            station_results.setdefault(station_code, []).append(df)
        except Exception as e:
            # Log the error but keep processing other files
            print(f"  ERROR — skipping: {e}")
        print()

    if not station_results:
        sys.exit("No files were processed successfully. Check errors above.")

    # --- Write one output CSV + zip per station ---
    print(f"Writing output files to '{output_dir}':")
    for station_code, dfs in station_results.items():
        combined = pd.concat(dfs, ignore_index=True)

        # Write the CSV
        csv_out = output_dir / f"{station_code}_datastream.csv"
        combined.to_csv(csv_out, index=False)

        # Create a zipped copy alongside it
        zip_out = zip_csv(csv_out)

        print(f"  {csv_out.name}: {len(combined):,} rows written")
        print(f"  {zip_out.name}: zipped ({zip_out.stat().st_size / 1024:.0f} KB)")

    print("\nDone! Send a sample output to Nell for review before uploading.")


if __name__ == "__main__":
    main()
