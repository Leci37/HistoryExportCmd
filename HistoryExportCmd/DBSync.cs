using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Serilog;

namespace HistoryExportCmd
{
        internal class DBSync
        {
                public DBSync(Serilog.ILogger logger, string cnPointsHistory)
                {
                        this.mCnPHistory = cnPointsHistory;
                        this._log = logger.ForContext(this.GetType());
                }

                /// <summary>
                /// Synchronizes point configuration records between the primary and backup servers.
                /// </summary>
                /// <param name="primaryServer">Name of the primary server.</param>
                /// <param name="backupServer">Name of the backup server.</param>
                /// <returns><c>true</c> if the synchronization succeeds; otherwise, <c>false</c>.</returns>
                public async Task<bool> SyncPointAsync(string primaryServer, string backupServer)
                {
                        bool res = false;
                        try
                        {
                                this._log.Information("Synchronizing Point table");
                                using (SqlConnection con = new SqlConnection(this.mCnPHistory))
                                {
                                        await con.OpenAsync().ConfigureAwait(false);
                                        using (SqlCommand cmd = con.CreateCommand())
                                        {
                                                cmd.CommandText = string.Format("insert into {1}.PointsHistory.dbo.Point select P.* from {0}.PointsHistory.dbo.Point P left join {1}.PointsHistory.dbo.Point B on P.PointId = B.PointId where B.PointId is null", primaryServer, backupServer);
                                                this._log.Debug("Executing SQL: {SQL}", cmd.CommandText);
                                                int affected = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                                                this._log.Information("{RecordCount} records inserted in backup server", affected);
                                        }
                                        using (SqlCommand cmd2 = con.CreateCommand())
                                        {
                                                cmd2.CommandText = string.Format("update B set B.PointName = P.PointName, B.ParamName = P.ParamName, B.Description = P.Description, B.Device = P.Device, B.HistoryFast = P.HistoryFast, B.HistorySlow = P.HistorySlow, B.HistoryExtd = P.HistoryExtd, B.HistoryFastArch = P.HistoryFastArch, B.HistorySlowArch = P.HistorySlowArch, B.HistoryExtdArch = P.HistoryExtdArch from {0}.PointsHistory.dbo.Point P join {1}.PointsHistory.dbo.Point B on P.PointId = B.PointId", primaryServer, backupServer);
                                                this._log.Debug("Executing SQL: {SQL}", cmd2.CommandText);
                                                int affected2 = await cmd2.ExecuteNonQueryAsync().ConfigureAwait(false);
                                                this._log.Information("{RecordCount} records updated in backup server", affected2);
                                        }
                                }
                                res = true;
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to synchronize Point table");
                        }
                        return res;
                }

                /// <summary>
                /// Synchronizes history data between the primary and backup servers for the provided cadence.
                /// </summary>
                /// <param name="primaryServer">Name of the primary server.</param>
                /// <param name="backupServer">Name of the backup server.</param>
                /// <param name="historyType">History cadence to synchronize.</param>
                /// <returns><c>true</c> if the synchronization finishes successfully; otherwise, <c>false</c>.</returns>
                public async Task<bool> SyncHistoryTableAsync(string primaryServer, string backupServer, HistoryType historyType)
                {
                        bool res = false;
                        string table = historyType switch
                        {
                                HistoryType.Fast => "History_5sec",
                                HistoryType.Slow => "History_1min",
                                HistoryType.Extended => "History_1hour",
                                _ => throw new ArgumentOutOfRangeException(nameof(historyType), historyType, null)
                        };
                        DateTime minDatetime = new DateTime(2000, 1, 1);
                        DateTime maxDatetime = new DateTime(2000, 1, 1);
                        try
                        {
                                this._log.Information("Synchronizing {Table} table", table);
                                using (SqlConnection con = new SqlConnection(this.mCnPHistory))
                                {
                                        await con.OpenAsync().ConfigureAwait(false);
                                        using (SqlCommand cmd = con.CreateCommand())
                                        {
                                                cmd.CommandText = string.Format("SELECT MAX(USTTimestamp) FROM {0}.PointsHistory.dbo.{1}", primaryServer, table);
                                                using (SqlDataReader dr = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                {
                                                        if (await dr.ReadAsync().ConfigureAwait(false) && !await dr.IsDBNullAsync(0).ConfigureAwait(false))
                                                        {
                                                                maxDatetime = dr.GetDateTime(0);
                                                        }
                                                }
                                                cmd.CommandText = string.Format("SELECT MAX(USTTimestamp) FROM {0}.PointsHistory.dbo.{1}", backupServer, table);
                                                using (SqlDataReader dr2 = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                                                {
                                                        if (await dr2.ReadAsync().ConfigureAwait(false) && !await dr2.IsDBNullAsync(0).ConfigureAwait(false))
                                                        {
                                                                minDatetime = dr2.GetDateTime(0).AddHours(-12.0);
                                                        }
                                                }
                                                cmd.Parameters.Clear();
                                                cmd.Parameters.Add("@MinTimestamp", SqlDbType.DateTime);
                                                cmd.Parameters.Add("@MaxTimestamp", SqlDbType.DateTime);
                                                while (minDatetime < maxDatetime)
                                                {
                                                        cmd.CommandText = string.Format("insert into {1}.PointsHistory.dbo.{2} select P.* from {0}.PointsHistory.dbo.{2} P left join {1}.PointsHistory.dbo.{2} B on P.PointId = B.PointId and P.USTTimestamp = B.USTTimestamp where P.USTTimestamp > @MinTimestamp and P.USTTimestamp <= @MaxTimestamp and B.PointId is null", primaryServer, backupServer, table);
                                                        DateTime auxDatetime = minDatetime.AddHours(1.0);
                                                        cmd.Parameters["@MinTimestamp"].Value = minDatetime;
                                                        cmd.Parameters["@MaxTimestamp"].Value = auxDatetime;
                                                        this._log.Debug("Executing SQL: {SQL}", cmd.CommandText);
                                                        int affected = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                                                        this._log.Information("{RecordCount} records inserted in backup server", affected);
                                                        minDatetime = auxDatetime;
                                                }
                                        }
                                }
                                res = true;
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to synchronize history table {Table}", table);
                        }
                        return res;
                }

                private readonly string mCnPHistory;

                private readonly Serilog.ILogger _log;
        }
}
