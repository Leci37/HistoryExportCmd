# HistoryExportCmd

Project recovery of a legacy .NET app from a compiled .exe, as the original source code is lost. The goal is to decompile, stabilize (fix DB errors), refactor the C# code, add modern logging and documentation, and implement new features to restore and improve the application.

## Configuration

Application settings now live in `appsettings.json`, with optional overrides in `appsettings.{Environment}.json`. The runtime environment is controlled through the `DOTNET_ENVIRONMENT` variable. The most common values are `Production` (default) and `Development`.

| Section | Setting | Description |
| --- | --- | --- |
| `Logging` | `Path` | Directory used for Serilog rolling log files. |
|  | `RetentionDays` | Number of log files to keep before they are removed. |
| `Processing` | `OldestDayFromToday` | Maximum age (in days) of history to process. |
|  | `PointBatchSize` | Number of points processed per batch when exporting history. |
| `Redundancy` | `Enabled` | Enables synchronization from the redundant server when `true`. |
| `ConnectionStrings` | `PointsHistory` | SQL Server connection string for the PointsHistory database. |
|  | `EBI_ODBC` | ODBC connection string for the EBI history snapshots. |
|  | `EBI_SQL` | SQL Server connection string used to query the EBI status. |

Create an `appsettings.Development.json` (one is provided as an example) to store machine-specific overrides without modifying the default settings.

## How to Run

1. Restore NuGet packages and build the solution:
   ```bash
   dotnet restore
   dotnet build
   ```
2. Configure the desired environment variables (e.g., `DOTNET_ENVIRONMENT=Development`).
3. Update the connection strings in the appropriate `appsettings*.json` files.
4. Execute the command-line application:
   ```bash
   dotnet run --project HistoryExportCmd/HistoryExportCmd.csproj
   ```

## Architecture Overview

The export process has two operating modes:

- **Primary mode** – When the current server is detected as the primary EBI instance, the application reads configured points, retrieves historical snapshots for each history cadence (fast, slow, extended), and persists the data into the PointsHistory database in batches.
- **Secondary mode** – When the server is not primary and redundancy is enabled, the application synchronizes point metadata and history tables from the opposing server (suffix `A`/`B`) to keep the backup environment aligned with the primary.

History cadence handling is now expressed through the `HistoryType` enum, making the code paths for fast (5 seconds), slow (1 minute), and extended (1 hour) histories explicit and type-safe.
