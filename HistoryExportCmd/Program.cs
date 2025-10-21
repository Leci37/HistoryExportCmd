using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using Serilog;

namespace HistoryExportCmd
{
	// Token: 0x02000007 RID: 7
	internal class Program : IDisposable
	{
		// Token: 0x06000020 RID: 32 RVA: 0x00003A70 File Offset: 0x00001C70
		private static int Main(string[] args)
		{
			int ret = 0;
			using (Program program = new Program())
			{
				ret = program.DoWork();
			}
			return ret;
		}

		// Token: 0x06000021 RID: 33 RVA: 0x00003AAC File Offset: 0x00001CAC
		private Program()
		{
			Directory.SetCurrentDirectory(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
			NameValueCollection config = ConfigurationManager.AppSettings;
                        string logPath = config["LogPath"] ?? "Logs";
                        Directory.CreateDirectory(logPath);
                        string logTemplate = "{Timestamp:G} [{Level:u3}] {Message:lj}{NewLine}{Exception}";
                        int retainedFiles = Convert.ToInt32(config["LogMaxDays"] ?? "15");
                        Log.Logger = new LoggerConfiguration()
                                .MinimumLevel.Debug()
                                .Enrich.FromLogContext()
                                .WriteTo.File(Path.Combine(logPath, "HistoryExportCmd_#.log"), outputTemplate: logTemplate, rollingInterval: RollingInterval.Day, retainedFileCountLimit: retainedFiles)
                                .CreateLogger();
                        this._log = Log.Logger.ForContext<Program>();
                        this._log.Information("Starting");
                }

		// Token: 0x06000022 RID: 34 RVA: 0x00003B25 File Offset: 0x00001D25
		public void Dispose()
		{
                        this._log.Information("Finished");
                        Log.CloseAndFlush();
                }

		// Token: 0x06000023 RID: 35 RVA: 0x00003B40 File Offset: 0x00001D40
		private int DoWork()
		{
			string cnPHistory = ConfigurationManager.ConnectionStrings["PointsHistory"].ConnectionString;
			string cnEbiOdbc = ConfigurationManager.ConnectionStrings["EBI_ODBC"].ConnectionString;
			string cnEbiSql = ConfigurationManager.ConnectionStrings["EBI_SQL"].ConnectionString;
                        DBAccess dbaccess = new DBAccess(this._log, cnEbiSql, cnEbiOdbc, cnPHistory);
			bool primary;
			bool ebistatus = dbaccess.GetEBIStatus(out primary);
			if (ebistatus)
			{
				if (primary)
				{
					this.Process(dbaccess);
				}
				else
				{
					this.Synchronize();
				}
			}
			else
			{
                                this._log.Information("Failure reading the EBI status, the process cannot run");
			}
			if (!ebistatus)
			{
				return 1;
			}
			return 0;
		}

		// Token: 0x06000024 RID: 36 RVA: 0x00003BD8 File Offset: 0x00001DD8
		private void Process(DBAccess dbaccess)
		{
                        this._log.Information("Starting the process");
			DateTime now = DateTime.Now.ToUniversalTime();
			int oldestDayFromToday = Convert.ToInt32(ConfigurationManager.AppSettings["OldestDayFromToday"]);
			DateTime oldestDay = now.Date.AddDays((double)(-(double)oldestDayFromToday));
			List<Point> points;
			bool res = dbaccess.GetPoints(out points);
			if (res)
			{
                                this._log.Information("{PointCount} points read from database", points.Count);
				for (int type = 1; type <= 3; type++)
				{
                                        this._log.Information("Working on {HistoryType}", (type == 1) ? "Fast History" : ((type == 2) ? "Standard History" : "Extended History"));
					List<Point> points2 = null;
					if (type == 1)
					{
						points2 = (from p in points
						where p.HistoryFast && p.HistoryFastArch
						select p).ToList<Point>();
					}
					if (type == 2)
					{
						points2 = (from p in points
						where p.HistorySlow && p.HistorySlowArch
						select p).ToList<Point>();
					}
					if (type == 3)
					{
						points2 = (from p in points
						where p.HistoryExtd && p.HistoryExtdArch
						select p).ToList<Point>();
					}
                                        this._log.Information("{PointCount} points configured for this History type", points2.Count);
					if (points2.Count > 0)
					{
						int interval = 0;
						if (type == 1)
						{
							interval = 5;
						}
						if (type == 2)
						{
							interval = 60;
						}
						if (type == 3)
						{
							interval = 3600;
						}
						DateTime iniDatetime;
						res = dbaccess.GetLastDatetime(type, out iniDatetime);
						if (res)
						{
							iniDatetime = ((iniDatetime < oldestDay) ? oldestDay : iniDatetime.AddSeconds(3.0));
							DateTime limit = now.AddMinutes(-130.0);
							while (iniDatetime < limit && res)
							{
								DateTime endDatetime = iniDatetime.AddSeconds((double)(3200 * interval));
								if (endDatetime > limit)
								{
									endDatetime = limit;
								}
                                                                this._log.Information("iniDateTime: {IniDateTime}", iniDatetime);
                                                                this._log.Information("endDateTime: {EndDateTime}", endDatetime);
								if (endDatetime - iniDatetime > TimeSpan.FromSeconds((double)interval))
								{
									res = dbaccess.Prepare();
									if (res)
									{
										int num = 10;
										int idx = 0;
										while (idx < points2.Count && res)
										{
											List<Point> points3 = new List<Point>();
											int idx2 = 0;
											while (idx2 < num && idx + idx2 < points2.Count)
											{
												points3.Add(points2[idx + idx2]);
												idx2++;
											}
											List<History> history;
											res = dbaccess.GetHistory(type, iniDatetime, endDatetime, points3, out history);
											if (res)
											{
												res = dbaccess.StoreHistory(type, history);
											}
											idx += num;
										}
										if (res)
										{
											res = dbaccess.Finish(type);
										}
									}
								}
								iniDatetime = endDatetime;
							}
						}
					}
				}
			}
                        this._log.Information("Process finished");
		}

		// Token: 0x06000025 RID: 37 RVA: 0x00003F3C File Offset: 0x0000213C
		private void Synchronize()
		{
			if (Convert.ToBoolean(ConfigurationManager.AppSettings["RedundantPointHistory"]))
			{
				string bServer = Environment.MachineName;
				string pServer;
				if (bServer.EndsWith("A"))
				{
					pServer = bServer.Substring(0, bServer.Length - 1) + "B";
				}
				else
				{
					pServer = bServer.Substring(0, bServer.Length - 1) + "A";
				}
                                this._log.Information("Starting the process");
                                string cnPHistory = ConfigurationManager.ConnectionStrings["PointsHistory"].ConnectionString;
                                DBSync dbaccess = new DBSync(this._log, cnPHistory);
				bool res = dbaccess.SyncPoint(pServer, bServer);
				int historyType = 1;
				while (historyType <= 3 && res)
				{
					res = dbaccess.SyncHistoryTable(pServer, bServer, historyType);
					historyType++;
				}
                                this._log.Information("Process finished");
			}
		}

		// Token: 0x04000018 RID: 24
                private readonly Serilog.ILogger _log;
	}
}
