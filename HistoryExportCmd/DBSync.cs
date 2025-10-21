using System;
using System.Data;
using System.Data.SqlClient;
using Serilog;

namespace HistoryExportCmd
{
	// Token: 0x02000004 RID: 4
	internal class DBSync
	{
		// Token: 0x06000011 RID: 17 RVA: 0x0000309C File Offset: 0x0000129C
                public DBSync(Serilog.ILogger logger, string cnPointsHistory)
                {
                        this.mCnPHistory = cnPointsHistory;
                        this._log = logger.ForContext(this.GetType());
                }

		// Token: 0x06000012 RID: 18 RVA: 0x000030B4 File Offset: 0x000012B4
		public bool SyncPoint(string pServer, string bServer)
		{
			bool res = false;
			try
			{
                                this._log.Information("Synchronizing Point table");
				using (SqlConnection con = new SqlConnection(this.mCnPHistory))
				{
					con.Open();
					using (SqlCommand cmd = con.CreateCommand())
					{
						cmd.CommandText = string.Format("insert into {1}.PointsHistory.dbo.Point select P.* from {0}.PointsHistory.dbo.Point P left join {1}.PointsHistory.dbo.Point B on P.PointId = B.PointId where b.PointId is null", pServer, bServer);
                                                this._log.Debug("Executing SQL: {SQL}", cmd.CommandText);
						int affected = cmd.ExecuteNonQuery();
                                                this._log.Information("{RecordCount} records inserted in backup server", affected);
					}
					using (SqlCommand cmd2 = con.CreateCommand())
					{
						cmd2.CommandText = string.Format("update B set B.PointName = P.PointName, B.ParamName = P.ParamName, B.Description = P.Description, B.Device = P.Device, B.HistoryFast = P.HistoryFast, B.HistorySlow = P.HistorySlow, B.HistoryExtd = P.HistoryExtd, B.HistoryFastArch = P.HistoryFastArch, B.HistorySlowArch = P.HistorySlowArch, B.HistoryExtdArch = P.HistoryExtdArch from {0}.PointsHistory.dbo.Point P join {1}.PointsHistory.dbo.Point B on P.PointId = B.PointId", pServer, bServer);
                                                this._log.Debug("Executing SQL: {SQL}", cmd2.CommandText);
						int affected2 = cmd2.ExecuteNonQuery();
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

		// Token: 0x06000013 RID: 19 RVA: 0x0000324C File Offset: 0x0000144C
		public bool SyncHistoryTable(string pServer, string bServer, int HistoryType)
		{
			bool res = false;
			string table = "";
			if (HistoryType == 1)
			{
				table = "History_5sec";
			}
			if (HistoryType == 2)
			{
				table = "History_1min";
			}
			if (HistoryType == 3)
			{
				table = "History_1hour";
			}
			DateTime minDatetime = new DateTime(2000, 1, 1);
			DateTime maxDatetime = new DateTime(2000, 1, 1);
			try
			{
                                this._log.Information("Synchronizing {Table} table", table);
				using (SqlConnection con = new SqlConnection(this.mCnPHistory))
				{
					con.Open();
					using (SqlCommand cmd = con.CreateCommand())
					{
						cmd.CommandText = string.Format("SELECT MAX(USTTimestamp) FROM {0}.PointsHistory.dbo.{1}", pServer, table);
						using (SqlDataReader dr = cmd.ExecuteReader())
						{
							if (dr.Read() && !dr.IsDBNull(0))
							{
								maxDatetime = dr.GetDateTime(0);
							}
						}
						cmd.CommandText = string.Format("SELECT MAX(USTTimestamp) FROM {0}.PointsHistory.dbo.{1}", bServer, table);
						using (SqlDataReader dr2 = cmd.ExecuteReader())
						{
							if (dr2.Read() && !dr2.IsDBNull(0))
							{
								minDatetime = dr2.GetDateTime(0).AddHours(-12.0);
							}
						}
						cmd.Parameters.Add("@MinTimestamp", SqlDbType.DateTime);
						cmd.Parameters.Add("@MaxTimestamp", SqlDbType.DateTime);
						DateTime now = DateTime.Now;
						while (minDatetime < maxDatetime)
						{
							cmd.CommandText = string.Format("insert into {1}.PointsHistory.dbo.{2} select P.* from {0}.PointsHistory.dbo.{2} P left join {1}.PointsHistory.dbo.{2} B on P.PointId = B.PointId and P.USTTimestamp = B.USTTimestamp where P.USTTimestamp > @MinTimestamp and P.USTTimestamp <= @MaxTimestamp and B.PointId is null", pServer, bServer, table);
							DateTime auxDatetime = minDatetime.AddHours(1.0);
							cmd.Parameters["@MinTimestamp"].Value = minDatetime;
							cmd.Parameters["@MaxTimestamp"].Value = auxDatetime;
                                                        this._log.Debug("Executing SQL: {SQL}", cmd.CommandText);
							int affected = cmd.ExecuteNonQuery();
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

		// Token: 0x04000007 RID: 7
		private string mCnPHistory;

		// Token: 0x04000008 RID: 8
                private readonly Serilog.ILogger _log;
	}
}
