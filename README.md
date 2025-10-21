# HistoryExportCmd

`HistoryExportCmd` is a .NET 4.0 console application designed to export historical data from an EBI system into a dedicated SQL Server database (`PointsHistory`). It includes logic for running in a redundant server environment.

**Project Status:** This is a project recovery from a decompiled .NET executable. The original source code was lost. The code has been stabilized but was not originally authored for this repository.

## Architecture & Core Functionality

The application operates in one of two modes, determined at startup:

1.  **Primary Mode:** If the application detects it is running on the primary EBI server (by successfully calling `hwsystem.dbo.hsc_sp_IsPrimary` or `hsc_mfn_IsPrimary`), it will:
    * Read point configurations from the local `PointsHistory` database (`Point` table).
    * Connect to the EBI history database via **ODBC** (`EBI_ODBC`).
    * Fetch data from `History5SecondSnapshot`, `History1MinSnapshot`, and `History1HourSnapshot` tables based on the last recorded timestamp.
    * Insert this data into the corresponding `History_5sec`, `History_1min`, and `History_1hour` tables in the `PointsHistory` SQL database.
    * Run an aggregation step to populate `History_15min` from `History_1min`.

2.  **Secondary (Sync) Mode:** If the EBI server is not primary and `RedundantPointHistory` is enabled in `app.config`, it will:
    * Assume a paired server (e.g., "SERVERA" and "SERVERB").
    * Connect to the *primary* server's `PointsHistory` database (likely via a linked server).
    * Synchronize the `Point` table and all `History_*` tables by pulling missing records from the primary to the local secondary database.

## Configuration (app.config)

All configuration is managed in `app.config`.

### Connection Strings
* `PointsHistory`: SQL Server connection string for the local database where history is stored.
* `EBI_ODBC`: The ODBC Data Source Name (DSN) for connecting to the EBI history snapshot tables.
* `EBI_SQL`: SQL Server connection string to the local EBI `master` database, used *only* to check for primary server status.

### App Settings
* `OldestDayFromToday`: The maximum number of days in the past to query for history if the local database is empty (e.g., `1295` days).
* `RedundantPointHistory`: `true`/`false`. Enables or disables the Secondary (Sync) Mode.
* `LogPath`: Directory to store log files (e.g., `Logs`).

## Key Classes
* `Program.cs`: Main entry point. Contains the high-level Primary/Secondary logic.
* `DBAccess.cs`: Handles all database operations for **Primary Mode** (reading from EBI ODBC, writing to `PointsHistory` SQL).
* `DBSync.cs`: Handles all database operations for **Secondary Mode** (syncing `PointsHistory` from the primary server).
* `LogFile.cs`: A simple custom file logger.
* `Point.cs` / `History.cs`: Data models for point configuration and history records.