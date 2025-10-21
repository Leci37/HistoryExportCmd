using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Serilog;

namespace HistoryExportCmd
{
        internal class DBAccess
        {
                public DBAccess(Serilog.ILogger logger, string cnEbiSql, string cnEbiOdbc, string cnPointsHistory)
                {
                        this.mCnEbiSql = cnEbiSql;
                        this.mCnEbiOdbc = cnEbiOdbc;
                        this.mCnPHistory = cnPointsHistory;
                        this._log = logger.ForContext(this.GetType());
                }

                /// <summary>
                /// Aggregates history data into fifteen-minute buckets within the specified window.
                /// </summary>
                /// <param name="iniTimestamp">Start of the time window.</param>
                /// <param name="endTimestamp">End of the time window.</param>
                /// <returns><c>true</c> if the aggregation completes successfully; otherwise, <c>false</c>.</returns>
                public async Task<bool> CalculateAsync(DateTime iniTimestamp, DateTime endTimestamp)
                {
                        bool res = false;
                        Console.WriteLine("Calculate: from {0} to {1}", iniTimestamp, endTimestamp.AddMinutes(-1.0));
                        try
                        {
                                using (SqlConnection con = new SqlConnection(this.mCnPHistory))
                                {
                                        await con.OpenAsync().ConfigureAwait(false);
                                        using (SqlCommand cmd = con.CreateCommand())
                                        {
                                                const string SQL = "INSERT INTO History_15min (USTTimestamp, Timestamp, PointName, ParamName, Value) SELECT DATEADD(MINUTE,(DATEPART(MINUTE,USTTimestamp)/15)*15,DATEADD(HOUR,DATEPART(HOUR,USTTimestamp),DATEADD(DAY,DATEPART(DAY,USTTimestamp)-1,DATEADD(MONTH,DATEPART(MONTH,USTimestamp)-1,DATEADD(YEAR,DATEPART(YEAR,USTTimestamp)-1900,0))))) USTTimestamp, DATEADD(MINUTE,(DATEPART(MINUTE,Timestamp)/15)*15,DATEADD(HOUR,DATEPART(HOUR,Timestamp),DATEADD(DAY,DATEPART(DAY,Timestamp)-1,DATEADD(MONTH,DATEPART(MONTH,Timestamp)-1,DATEADD(YEAR,DATEPART(YEAR,Timestamp)-1900,0))))) Timestamp, PointName, ParamName, AVG(Value) Avg FROM History_1min WHERE USTTimestamp >= @FROM AND USTTimestamp < @TO GROUP BY DATEADD(MINUTE,(DATEPART(MINUTE,USTTimestamp)/15)*15,DATEADD(HOUR,DATEPART(HOUR,USTTimestamp),DATEADD(DAY,DATEPART(DAY,USTTimestamp)-1,DATEADD(MONTH,DATEPART(MONTH,USTimestamp)-1,DATEADD(YEAR,DATEPART(YEAR,USTTimestamp)-1900,0))))), DATEADD(MINUTE,(DATEPART(MINUTE,Timestamp)/15)*15,DATEADD(HOUR,DATEPART(HOUR,Timestamp),DATEADD(DAY,DATEPART(DAY,Timestamp)-1,DATEADD(MONTH,DATEPART(MONTH,Timestamp)-1,DATEADD(YEAR,DATEPART(YEAR,Timestamp)-1900,0))))), PointName, ParamName ";
                                                cmd.CommandText = SQL;
                                                cmd.Parameters.AddWithValue("FROM", iniTimestamp);
                                                cmd.Parameters.AddWithValue("TO", endTimestamp);
                                                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                                        }
                                        res = true;
                                }
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Error calculating history between {Start} and {End}", iniTimestamp, endTimestamp);
                        }
                        return res;
                }

                /// <summary>
                /// Determines whether the EBI instance connected through SQL Server is primary.
                /// </summary>
                /// <returns>A tuple indicating whether the operation succeeded and the primary status.</returns>
                public async Task<(bool Success, bool Primary)> GetEBIStatusAsync()
                {
                        bool primary = false;
                        bool res;
                        try
                        {
                                using (SqlConnection con = new SqlConnection(this.mCnEbiSql))
                                {
                                        await con.OpenAsync().ConfigureAwait(false);
                                        using (SqlCommand cmd = con.CreateCommand())
                                        {
                                                if (this.mFnType == 0)
                                                {
                                                        cmd.CommandText = "SELECT OBJECT_ID('hwsystem.dbo.hsc_sp_IsPrimary')";
                                                        using (SqlDataReader dr = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                        {
                                                                if (await dr.ReadAsync().ConfigureAwait(false) && !await dr.IsDBNullAsync(0).ConfigureAwait(false))
                                                                {
                                                                        this.mFnType = 1;
                                                                }
                                                        }
                                                }
                                                if (this.mFnType == 0)
                                                {
                                                        cmd.CommandText = "SELECT OBJECT_ID('hwsystem.dbo.hsc_mfn_IsPrimary')";
                                                        using (SqlDataReader dr2 = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                        {
                                                                if (await dr2.ReadAsync().ConfigureAwait(false) && !await dr2.IsDBNullAsync(0).ConfigureAwait(false))
                                                                {
                                                                        this.mFnType = 2;
                                                                }
                                                        }
                                                }
                                                if (this.mFnType == 0)
                                                {
                                                        this._log.Error("Couldn't find the method to determine the primary EBI");
                                                }
                                                if (this.mFnType == 1)
                                                {
                                                        const string SQL = "EXEC hwsystem.dbo.hsc_sp_IsPrimary";
                                                        cmd.CommandText = SQL;
                                                        this._log.Debug("Executing SQL: {SQL}", cmd.CommandText);
                                                        using (SqlDataReader dr3 = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                        {
                                                                if (await dr3.ReadAsync().ConfigureAwait(false))
                                                                {
                                                                        primary = dr3.GetInt16(0) != 0;
                                                                }
                                                        }
                                                }
                                                if (this.mFnType == 2)
                                                {
                                                        const string SQL2 = "SELECT hwsystem.dbo.hsc_mfn_IsPrimary()";
                                                        cmd.CommandText = SQL2;
                                                        this._log.Debug("Executing SQL: {SQL}", cmd.CommandText);
                                                        using (SqlDataReader dr4 = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                        {
                                                                if (await dr4.ReadAsync().ConfigureAwait(false))
                                                                {
                                                                        primary = dr4.GetBoolean(0);
                                                                }
                                                        }
                                                }
                                        }
                                        res = true;
                                }
                        }
                        catch (Exception ex)
                        {
                                this._log.Error(ex, "Failed to get EBI status");
                                res = false;
                        }
                        return (res, primary);
                }

                /// <summary>
                /// Retrieves the last timestamp stored for the specified history table.
                /// </summary>
                /// <param name="historyType">The history cadence to query.</param>
                /// <returns>A tuple indicating success and the retrieved timestamp when available.</returns>
                public async Task<(bool Success, DateTime? LastDatetime)> GetLastDatetimeAsync(HistoryType historyType)
                {
                        bool res = false;
                        DateTime? lastDatetime = null;
                        try
                        {
                                using (SqlConnection con = new SqlConnection(this.mCnPHistory))
                                {
                                        await con.OpenAsync().ConfigureAwait(false);
                                        using (SqlCommand cmd = con.CreateCommand())
                                        {
                                                cmd.CommandText = historyType switch
                                                {
                                                        HistoryType.Fast => "SELECT MAX(USTTimestamp) FROM History_5sec",
                                                        HistoryType.Slow => "SELECT MAX(USTTimestamp) FROM History_1min",
                                                        HistoryType.Extended => "SELECT MAX(USTTimestamp) FROM History_1hour",
                                                        _ => throw new ArgumentOutOfRangeException(nameof(historyType), historyType, null)
                                                };
                                                using (SqlDataReader dr = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                {
                                                        if (await dr.ReadAsync().ConfigureAwait(false) && !await dr.IsDBNullAsync(0).ConfigureAwait(false))
                                                        {
                                                                lastDatetime = dr.GetDateTime(0);
                                                        }
                                                        res = true;
                                                }
                                        }
                                }
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to get last datetime for history type {HistoryType}", historyType);
                        }
                        return (res, lastDatetime);
                }

                /// <summary>
                /// Reads a boolean parameter from the configuration table.
                /// </summary>
                /// <param name="name">Name of the parameter to look up.</param>
                /// <returns>A tuple indicating success and the parameter value.</returns>
                public async Task<(bool Success, bool Value)> GetParameterAsync(string name)
                {
                        bool res = false;
                        bool value = false;
                        try
                        {
                                using (SqlConnection con = new SqlConnection(this.mCnPHistory))
                                {
                                        await con.OpenAsync().ConfigureAwait(false);
                                        using (SqlCommand cmd = con.CreateCommand())
                                        {
                                                cmd.CommandText = "SELECT Value FROM Parameter WHERE Name = @Name";
                                                cmd.Parameters.AddWithValue("@Name", name);
                                                using (SqlDataReader dr = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                {
                                                        if (await dr.ReadAsync().ConfigureAwait(false) && !await dr.IsDBNullAsync(0).ConfigureAwait(false))
                                                        {
                                                                value = Convert.ToInt32(dr.GetString(0)) != 0;
                                                                res = true;
                                                        }
                                                }
                                        }
                                }
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to get parameter {ParameterName}", name);
                        }
                        return (res, value);
                }

                /// <summary>
                /// Retrieves the list of points configured for history export.
                /// </summary>
                /// <returns>A tuple indicating success and the set of configured points.</returns>
                public async Task<(bool Success, List<Point> Points)> GetPointsAsync()
                {
                        bool res = false;
                        List<Point> points = null;
                        try
                        {
                                using (SqlConnection con = new SqlConnection(this.mCnPHistory))
                                {
                                        await con.OpenAsync().ConfigureAwait(false);
                                        using (SqlCommand cmd = con.CreateCommand())
                                        {
                                                const string SQL = "SELECT PointId, PointName, ParamName, HistoryFast, HistorySlow, HistoryExtd, HistoryFastArch, HistorySlowArch, HistoryExtdArch FROM Point WHERE ((HistoryFast = 1) AND (HistoryFastArch = 1)) OR ((HistorySlow = 1) AND (HistorySlowArch = 1)) OR ((HistoryExtd = 1) AND (HistoryExtdArch = 1))";
                                                cmd.CommandText = SQL;
                                                this._log.Debug("Executing SQL: {SQL}", SQL);
                                                using (SqlDataReader dr = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                {
                                                        points = new List<Point>();
                                                        while (await dr.ReadAsync().ConfigureAwait(false))
                                                        {
                                                                Point point = new Point
                                                                {
                                                                        PointId = dr.GetInt32(0),
                                                                        PointName = dr.GetString(1),
                                                                        ParamName = dr.GetString(2),
                                                                        HistoryFast = dr.GetNullableBoolean(3).GetValueOrDefault(),
                                                                        HistorySlow = dr.GetNullableBoolean(4).GetValueOrDefault(),
                                                                        HistoryExtd = dr.GetNullableBoolean(5).GetValueOrDefault(),
                                                                        HistoryFastArch = dr.GetNullableBoolean(6).GetValueOrDefault(),
                                                                        HistorySlowArch = dr.GetNullableBoolean(7).GetValueOrDefault(),
                                                                        HistoryExtdArch = dr.GetNullableBoolean(8).GetValueOrDefault()
                                                                };
                                                                points.Add(point);
                                                        }
                                                        res = true;
                                                }
                                        }
                                }
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to get points");
                        }
                        return (res, points ?? new List<Point>());
                }

                /// <summary>
                /// Pulls history data from the EBI ODBC source for the provided points and window.
                /// </summary>
                /// <param name="historyType">The cadence of history to retrieve.</param>
                /// <param name="iniTimestamp">Start of the time window.</param>
                /// <param name="endTimestamp">End of the time window.</param>
                /// <param name="points">Points to retrieve history for.</param>
                /// <returns>A tuple indicating success and the collected history records.</returns>
                public async Task<(bool Success, List<History> History)> GetHistoryAsync(HistoryType historyType, DateTime iniTimestamp, DateTime endTimestamp, IReadOnlyList<Point> points)
                {
                        bool res = false;
                        List<History> lhistory = null;
                        string table = historyType switch
                        {
                                HistoryType.Fast => "History5SecondSnapshot",
                                HistoryType.Slow => "History1MinSnapshot",
                                HistoryType.Extended => "History1HourSnapshot",
                                _ => throw new ArgumentOutOfRangeException(nameof(historyType), historyType, null)
                        };
                        StringBuilder text = new StringBuilder(1000);
                        text.Append(string.Format("Get from {0}: since {1} to {2}, ", table, iniTimestamp, endTimestamp));
                        text.Append("Points: ");
                        for (int idx = 0; idx < points.Count; idx++)
                        {
                                if (idx > 0)
                                {
                                        text.Append(", ");
                                }
                                text.Append(points[idx].PointName + "." + points[idx].ParamName);
                        }
                        Console.WriteLine(text.ToString());
                        this._log.Information(text.ToString());
                        try
                        {
                                using (OdbcConnection con = new OdbcConnection(this.mCnEbiOdbc))
                                {
                                        await con.OpenAsync().ConfigureAwait(false);
                                        using (OdbcCommand cmd = con.CreateCommand())
                                        {
                                                StringBuilder SQL = new StringBuilder(500);
                                                SQL.Append("SELECT USTTimeStamp, TimeStamp, ");
                                                for (int idx2 = 0; idx2 < points.Count; idx2++)
                                                {
                                                        SQL.Append(string.Format("Parameter{0:00#}, Value{0:00#}, Quality{0:00#}", idx2 + 1));
                                                        if (idx2 < points.Count - 1)
                                                        {
                                                                SQL.Append(", ");
                                                        }
                                                        else
                                                        {
                                                                SQL.Append(" ");
                                                        }
                                                }
                                                SQL.Append("FROM " + table + " ");
                                                SQL.Append("WHERE USTTimeStamp >= ? AND USTTimeStamp < ? ");
                                                for (int idx3 = 0; idx3 < points.Count; idx3++)
                                                {
                                                        SQL.Append(string.Format("AND Parameter{0:00#} = '{1}.{2}' ", idx3 + 1, points[idx3].PointName, points[idx3].ParamName));
                                                }
                                                this._log.Debug("Executing SQL: {SQL}", SQL.ToString());
                                                cmd.CommandText = SQL.ToString();
                                                cmd.Parameters.AddWithValue("FROM", iniTimestamp);
                                                cmd.Parameters.AddWithValue("TO", endTimestamp);
                                                using (DbDataReader dr = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                {
                                                        lhistory = new List<History>();
                                                        while (await dr.ReadAsync().ConfigureAwait(false))
                                                        {
                                                                int i = 0;
                                                                DateTime usttimestamp = dr.GetDateTime(i++);
                                                                DateTime timestamp = dr.GetDateTime(i++);
                                                                for (int idx4 = 0; idx4 < points.Count; idx4++)
                                                                {
                                                                        string[] pointParam = dr.GetString(i++).Split('.');
                                                                        double value = dr.GetDouble(i++);
                                                                        string quality = dr.GetString(i++);
                                                                        if (quality == "GOOD")
                                                                        {
                                                                                Point point = points[idx4];
                                                                                if (!(point.PointName == pointParam[0] && point.ParamName == pointParam[1]))
                                                                                {
                                                                                        point = null;
                                                                                        foreach (Point candidate in points)
                                                                                        {
                                                                                                if (candidate.PointName == pointParam[0] && candidate.ParamName == pointParam[1])
                                                                                                {
                                                                                                        point = candidate;
                                                                                                        break;
                                                                                                }
                                                                                        }
                                                                                }
                                                                                if (point != null)
                                                                                {
                                                                                        History history = new History
                                                                                        {
                                                                                                PointId = point.PointId,
                                                                                                USTTimestamp = usttimestamp,
                                                                                                Timestamp = timestamp,
                                                                                                Value = value
                                                                                        };
                                                                                        lhistory.Add(history);
                                                                                }
                                                                        }
                                                                        else
                                                                        {
                                                                                this._log.Debug("Point Quality is not GOOD: {Point}.{Param} {Quality} {Timestamp}", pointParam[0], pointParam[1], quality, usttimestamp);
                                                                        }
                                                                }
                                                        }
                                                        Console.WriteLine("{0} records retrieved", lhistory.Count);
                                                        this._log.Information("{RecordCount} records retrieved", lhistory.Count);
                                                        res = true;
                                                }
                                        }
                                }
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to get history for type {HistoryType}", historyType);
                        }
                        return (res, lhistory ?? new List<History>());
                }

                /// <summary>
                /// Creates a staging table used to buffer history records before persistence.
                /// </summary>
                /// <returns><c>true</c> if the staging environment is ready; otherwise, <c>false</c>.</returns>
                public async Task<bool> PrepareAsync()
                {
                        bool res = false;
                        try
                        {
                                if (this.mConn != null)
                                {
                                        this.mConn.Dispose();
                                }
                                this.mConn = new SqlConnection(this.mCnPHistory);
                                await this.mConn.OpenAsync().ConfigureAwait(false);
                                using (SqlCommand cmd = this.mConn.CreateCommand())
                                {
                                        const string SQL = "CREATE TABLE #History (PointId int NOT NULL, USTTimestamp datetime NOT NULL, Timestamp datetime NULL, Value float NULL)";
                                        cmd.CommandText = SQL;
                                        this._log.Debug("Executing SQL: {SQL}", SQL);
                                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                                        res = true;
                                }
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to prepare history staging table");
                                if (this.mConn != null)
                                {
                                        this.mConn.Dispose();
                                        this.mConn = null;
                                }
                        }
                        return res;
                }

                /// <summary>
                /// Inserts history records into the staging table.
                /// </summary>
                /// <param name="historyType">The cadence of history being stored.</param>
                /// <param name="lhistory">Collection of history rows to persist.</param>
                /// <returns><c>true</c> when the records are stored successfully; otherwise, <c>false</c>.</returns>
                public async Task<bool> StoreHistoryAsync(HistoryType historyType, IReadOnlyList<History> lhistory)
                {
                        if (lhistory == null)
                        {
                                throw new ArgumentNullException(nameof(lhistory));
                        }

                        bool res = false;
                        Console.WriteLine("Store {0} records", lhistory.Count);
                        this._log.Information("Store {RecordCount} records", lhistory.Count);
                        try
                        {
                                if (this.mConn == null)
                                {
                                        return false;
                                }

                                using (SqlCommand cmd = this.mConn.CreateCommand())
                                {
                                        const string SQL = "INSERT INTO #History (PointId, USTTimestamp, Timestamp, Value) VALUES (@PointId, @USTTimestamp, @Timestamp, @Value)";
                                        cmd.CommandText = SQL;
                                        this._log.Debug("Executing SQL: {SQL}", SQL);
                                        foreach (History history in lhistory)
                                        {
                                                try
                                                {
                                                        cmd.Parameters.Clear();
                                                        cmd.Parameters.AddWithValue("@PointId", history.PointId);
                                                        cmd.Parameters.AddWithValue("@USTTimestamp", history.USTTimestamp);
                                                        cmd.Parameters.AddWithValue("@Timestamp", history.Timestamp);
                                                        cmd.Parameters.AddWithValue("@Value", history.Value);
                                                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                                                }
                                                catch (Exception ex)
                                                {
                                                        this._log.Error(ex, "Failed to insert history record for point {PointId} at {Timestamp}", history.PointId, history.USTTimestamp);
                                                }
                                        }
                                }
                                res = true;
                        }
                        catch (Exception ex2)
                        {
                                Console.WriteLine(ex2.Message);
                                this._log.Error(ex2, "Failed to store history records");
                        }
                        return res;
                }

                /// <summary>
                /// Moves staged history records into the final archive table for the requested cadence.
                /// </summary>
                /// <param name="historyType">The cadence of history being finalized.</param>
                /// <returns><c>true</c> if the move succeeds; otherwise, <c>false</c>.</returns>
                public async Task<bool> FinishAsync(HistoryType historyType)
                {
                        bool res = false;
                        string table = historyType switch
                        {
                                HistoryType.Fast => "History_5sec",
                                HistoryType.Slow => "History_1min",
                                HistoryType.Extended => "History_1hour",
                                _ => throw new ArgumentOutOfRangeException(nameof(historyType), historyType, null)
                        };
                        Console.WriteLine("Move records to {0}", table);
                        this._log.Information("Move records to {Table}", table);
                        try
                        {
                                if (this.mConn == null)
                                {
                                        return false;
                                }

                                using (SqlCommand cmd = this.mConn.CreateCommand())
                                {
                                        string SQL = "INSERT INTO " + table + " (PointId, USTTimestamp, Timestamp, Value) SELECT PointId, USTTimestamp, Timestamp, Value FROM #History";
                                        cmd.CommandText = SQL;
                                        cmd.CommandTimeout = 600;
                                        this._log.Debug("Executing SQL: {SQL}", SQL);
                                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                                }
                                this.mConn.Dispose();
                                this.mConn = null;
                                res = true;
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to finish processing for {Table}", table);
                        }
                        return res;
                }

                /// <summary>
                /// Clears any pending staging connection without moving staged rows to the final tables.
                /// </summary>
                public void Reset()
                {
                        if (this.mConn != null)
                        {
                                this.mConn.Dispose();
                                this.mConn = null;
                        }
                }

                private readonly string mCnPHistory;

                private readonly string mCnEbiOdbc;

                private readonly string mCnEbiSql;

                private int mFnType;

                private readonly Serilog.ILogger _log;

                private SqlConnection mConn;
        }
}
