using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Text;
using Serilog;

namespace HistoryExportCmd
{
	// Token: 0x02000002 RID: 2
	internal class DBAccess
	{
		// Token: 0x06000001 RID: 1 RVA: 0x00002050 File Offset: 0x00000250
                public DBAccess(Serilog.ILogger logger, string cnEbiSql, string cnEbiOdbc, string cnPointsHistory)
                {
                        this.mCnEbiSql = cnEbiSql;
                        this.mCnEbiOdbc = cnEbiOdbc;
                        this.mCnPHistory = cnPointsHistory;
                        this._log = logger.ForContext(this.GetType());
                }

		// Token: 0x06000002 RID: 2 RVA: 0x00002078 File Offset: 0x00000278
		public bool Calculate(DateTime iniTimestamp, DateTime endTimestamp)
		{
			bool res = false;
			Console.WriteLine("Calculate: from {0} to {1}", iniTimestamp, endTimestamp.AddMinutes(-1.0));
			try
			{
				using (SqlConnection con = new SqlConnection(this.mCnPHistory))
				{
					con.Open();
					using (SqlCommand cmd = con.CreateCommand())
					{
						string SQL = "INSERT INTO History_15min (USTTimestamp, Timestamp, PointName, ParamName, Value) SELECT DATEADD(MINUTE,(DATEPART(MINUTE,USTTimestamp)/15)*15,DATEADD(HOUR,DATEPART(HOUR,USTTimestamp),DATEADD(DAY,DATEPART(DAY,USTTimestamp)-1,DATEADD(MONTH,DATEPART(MONTH,USTTimestamp)-1,DATEADD(YEAR,DATEPART(YEAR,USTTimestamp)-1900,0))))) USTTimestamp, DATEADD(MINUTE,(DATEPART(MINUTE,Timestamp)/15)*15,DATEADD(HOUR,DATEPART(HOUR,Timestamp),DATEADD(DAY,DATEPART(DAY,Timestamp)-1,DATEADD(MONTH,DATEPART(MONTH,Timestamp)-1,DATEADD(YEAR,DATEPART(YEAR,Timestamp)-1900,0))))) Timestamp, PointName, ParamName, AVG(Value) Avg FROM History_1min WHERE USTTimestamp >= @FROM AND USTTimestamp < @TO GROUP BY DATEADD(MINUTE,(DATEPART(MINUTE,USTTimestamp)/15)*15,DATEADD(HOUR,DATEPART(HOUR,USTTimestamp),DATEADD(DAY,DATEPART(DAY,USTTimestamp)-1,DATEADD(MONTH,DATEPART(MONTH,USTTimestamp)-1,DATEADD(YEAR,DATEPART(YEAR,USTTimestamp)-1900,0))))), DATEADD(MINUTE,(DATEPART(MINUTE,Timestamp)/15)*15,DATEADD(HOUR,DATEPART(HOUR,Timestamp),DATEADD(DAY,DATEPART(DAY,Timestamp)-1,DATEADD(MONTH,DATEPART(MONTH,Timestamp)-1,DATEADD(YEAR,DATEPART(YEAR,Timestamp)-1900,0))))), PointName, ParamName ";
						cmd.CommandText = SQL;
						cmd.Parameters.AddWithValue("FROM", iniTimestamp);
						cmd.Parameters.AddWithValue("TO", endTimestamp);
						cmd.ExecuteNonQuery();
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

		// Token: 0x06000003 RID: 3 RVA: 0x0000216C File Offset: 0x0000036C
		public bool GetEBIStatus(out bool primary)
		{
			primary = false;
			bool res;
			try
			{
				using (SqlConnection con = new SqlConnection(this.mCnEbiSql))
				{
					con.Open();
					using (SqlCommand cmd = con.CreateCommand())
					{
						if (this.mFnType == 0)
						{
							cmd.CommandText = "SELECT OBJECT_ID('hwsystem.dbo.hsc_sp_IsPrimary')";
							using (SqlDataReader dr = cmd.ExecuteReader())
							{
								if (dr.Read() && !dr.IsDBNull(0))
								{
									this.mFnType = 1;
								}
							}
						}
						if (this.mFnType == 0)
						{
							cmd.CommandText = "SELECT OBJECT_ID('hwsystem.dbo.hsc_mfn_IsPrimary')";
							using (SqlDataReader dr2 = cmd.ExecuteReader())
							{
								if (dr2.Read() && !dr2.IsDBNull(0))
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
							string SQL = "EXEC hwsystem.dbo.hsc_sp_IsPrimary";
							cmd.CommandText = SQL;
                                                        this._log.Debug("Executing SQL: {SQL}", cmd.CommandText);
							using (SqlDataReader dr3 = cmd.ExecuteReader())
							{
								if (dr3.Read())
								{
									primary = (dr3.GetInt16(0) != 0);
								}
							}
						}
						if (this.mFnType == 2)
						{
							string SQL2 = "SELECT hwsystem.dbo.hsc_mfn_IsPrimary()";
							cmd.CommandText = SQL2;
                                                        this._log.Debug("Executing SQL: {SQL}", cmd.CommandText);
							using (SqlDataReader dr4 = cmd.ExecuteReader())
							{
								if (dr4.Read())
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
			return res;
		}

		// Token: 0x06000004 RID: 4 RVA: 0x000023C8 File Offset: 0x000005C8
		public bool GetLastDatetime(int HistoryType, out DateTime lastDatetime)
		{
			bool res = false;
			lastDatetime = new DateTime(2000, 1, 1);
			try
			{
				using (SqlConnection con = new SqlConnection(this.mCnPHistory))
				{
					con.Open();
					using (SqlCommand cmd = con.CreateCommand())
					{
						if (HistoryType == 1)
						{
							cmd.CommandText = "SELECT MAX(USTTimestamp) FROM History_5sec";
						}
						if (HistoryType == 2)
						{
							cmd.CommandText = "SELECT MAX(USTTimestamp) FROM History_1min";
						}
						if (HistoryType == 3)
						{
							cmd.CommandText = "SELECT MAX(USTTimestamp) FROM History_1hour";
						}
						using (SqlDataReader dr = cmd.ExecuteReader())
						{
							if (dr.Read() && !dr.IsDBNull(0))
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
                                this._log.Error(ex, "Failed to get last datetime for history type {HistoryType}", HistoryType);
                        }
			return res;
		}

		// Token: 0x06000005 RID: 5 RVA: 0x000024D0 File Offset: 0x000006D0
		public bool GetParameter(string name, out bool value)
		{
			bool res = false;
			value = false;
			try
			{
				using (SqlConnection con = new SqlConnection(this.mCnPHistory))
				{
					con.Open();
					using (SqlCommand cmd = con.CreateCommand())
					{
						cmd.CommandText = "SELECT Value FROM Parameter WHERE Name = @Name";
						cmd.Parameters.AddWithValue("@Name", name);
						using (SqlDataReader dr = cmd.ExecuteReader())
						{
							if (dr.Read() && !dr.IsDBNull(0))
							{
								value = (Convert.ToInt32(dr.GetString(0)) != 0);
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
			return res;
		}

		// Token: 0x06000006 RID: 6 RVA: 0x000025BC File Offset: 0x000007BC
		public bool GetPoints(out List<Point> points)
		{
			bool res = false;
			points = null;
			try
			{
				using (SqlConnection con = new SqlConnection(this.mCnPHistory))
				{
					con.Open();
					using (SqlCommand cmd = con.CreateCommand())
					{
						string SQL = "SELECT PointId, PointName, ParamName, HistoryFast, HistorySlow, HistoryExtd, HistoryFastArch, HistorySlowArch, HistoryExtdArch FROM Point WHERE ((HistoryFast = 1) AND (HistoryFastArch = 1)) OR ((HistorySlow = 1) AND (HistorySlowArch = 1)) OR ((HistoryExtd = 1) AND (HistoryExtdArch = 1))";
						cmd.CommandText = SQL;
                                                this._log.Debug("Executing SQL: {SQL}", SQL);
						using (SqlDataReader dr = cmd.ExecuteReader())
						{
							points = new List<Point>();
							while (dr.Read())
							{
								Point point = new Point();
								point.PointId = dr.GetInt32(0);
								point.PointName = dr.GetString(1);
								point.ParamName = dr.GetString(2);
								point.HistoryFast = dr.GetNullableBoolean(3).GetValueOrDefault();
								point.HistorySlow = dr.GetNullableBoolean(4).GetValueOrDefault();
								point.HistoryExtd = dr.GetNullableBoolean(5).GetValueOrDefault();
								point.HistoryFastArch = dr.GetNullableBoolean(6).GetValueOrDefault();
								point.HistorySlowArch = dr.GetNullableBoolean(7).GetValueOrDefault();
								point.HistoryExtdArch = dr.GetNullableBoolean(8).GetValueOrDefault();
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
			return res;
		}

		// Token: 0x06000007 RID: 7 RVA: 0x000027A0 File Offset: 0x000009A0
		public bool GetHistory(int HistoryType, DateTime iniTimestamp, DateTime endTimestamp, List<Point> points, out List<History> lhistory)
		{
			bool res = false;
			lhistory = null;
			string table = null;
			if (HistoryType == 1)
			{
				table = "History5SecondSnapshot";
			}
			if (HistoryType == 2)
			{
				table = "History1MinSnapshot";
			}
			if (HistoryType == 3)
			{
				table = "History1HourSnapshot";
			}
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
					con.Open();
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
						using (OdbcDataReader dr = cmd.ExecuteReader())
						{
							lhistory = new List<History>();
							while (dr.Read())
							{
								int i = 0;
								DateTime usttimestamp = dr.GetDateTime(i++);
								DateTime timestamp = dr.GetDateTime(i++);
								for (int idx4 = 0; idx4 < points.Count; idx4++)
								{
									string[] PointParam = dr.GetString(i++).Split(new char[]
									{
										'.'
									});
									double value = dr.GetDouble(i++);
									string quality = dr.GetString(i++);
									if (quality == "GOOD")
									{
										Point point = points.Find((Point p) => p.PointName == PointParam[0] && p.ParamName == PointParam[1]);
										if (point != null)
										{
											History history = new History();
											history.PointId = point.PointId;
											history.USTTimestamp = usttimestamp;
											history.Timestamp = timestamp;
											history.Value = value;
											lhistory.Add(history);
										}
									}
									else
									{
                                                                        this._log.Debug("Point Quality is not GOOD: {Point}.{Param} {Quality} {Timestamp}", PointParam[0], PointParam[1], quality, usttimestamp);
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
                                this._log.Error(ex, "Failed to get history for type {HistoryType}", HistoryType);
                        }
			return res;
		}

		// Token: 0x06000008 RID: 8 RVA: 0x00002C1C File Offset: 0x00000E1C
		public bool Prepare()
		{
			bool res = false;
			try
			{
				if (this.mConn != null)
				{
					this.mConn.Dispose();
				}
				this.mConn = new SqlConnection(this.mCnPHistory);
				this.mConn.Open();
				using (SqlCommand cmd = this.mConn.CreateCommand())
				{
					string SQL = "CREATE TABLE #History (PointId int NOT NULL, USTTimestamp datetime NOT NULL, Timestamp datetime NULL, Value float NULL)";
					cmd.CommandText = SQL;
                                        this._log.Debug("Executing SQL: {SQL}", SQL);
					cmd.ExecuteNonQuery();
					res = true;
				}
			}
                        catch (Exception ex)
                        {
                                Console.WriteLine(ex.Message);
                                this._log.Error(ex, "Failed to prepare history staging table");
                        }
			return res;
		}

		// Token: 0x06000009 RID: 9 RVA: 0x00002CD4 File Offset: 0x00000ED4
		public bool StoreHistory(int HistoryType, List<History> lhistory)
		{
			bool res = false;
			Console.WriteLine("Store {0} records", lhistory.Count);
                        this._log.Information("Store {RecordCount} records", lhistory.Count);
			try
			{
				using (SqlCommand cmd = this.mConn.CreateCommand())
				{
					string SQL = "INSERT INTO #History (PointId, USTTimestamp, Timestamp, Value) VALUES (@PointId, @USTTimestamp, @Timestamp, @Value)";
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
							cmd.ExecuteNonQuery();
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

		// Token: 0x0600000A RID: 10 RVA: 0x00002EAC File Offset: 0x000010AC
		public bool Finish(int HistoryType)
		{
			bool res = false;
			string table = null;
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
			Console.WriteLine("Move records to {0}", table);
                        this._log.Information("Move records to {Table}", table);
			try
			{
				using (SqlCommand cmd = this.mConn.CreateCommand())
				{
					string SQL = "INSERT INTO " + table + " (PointId, USTTimestamp, Timestamp, Value) SELECT PointId, USTTimestamp, Timestamp, Value FROM #History";
					cmd.CommandText = SQL;
					cmd.CommandTimeout = 600;
                                        this._log.Debug("Executing SQL: {SQL}", SQL);
					cmd.ExecuteNonQuery();
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

		// Token: 0x04000001 RID: 1
		private string mCnPHistory;

		// Token: 0x04000002 RID: 2
		private string mCnEbiOdbc;

		// Token: 0x04000003 RID: 3
		private string mCnEbiSql;

		// Token: 0x04000004 RID: 4
		private int mFnType;

		// Token: 0x04000005 RID: 5
                private readonly Serilog.ILogger _log;

		// Token: 0x04000006 RID: 6
		private SqlConnection mConn;
	}
}
