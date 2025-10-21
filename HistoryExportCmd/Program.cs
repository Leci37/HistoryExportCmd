using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace HistoryExportCmd
{
        internal class Program : IDisposable
        {
                private Program(IConfiguration configuration)
                {
                        string baseDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? Directory.GetCurrentDirectory();
                        Directory.SetCurrentDirectory(baseDirectory);

                        this._configuration = configuration;
                        string logPath = configuration.GetValue("Logging:Path", "Logs");
                        Directory.CreateDirectory(logPath);
                        string logTemplate = "{Timestamp:G} [{Level:u3}] {Message:lj}{NewLine}{Exception}";
                        int retainedFiles = Math.Max(1, configuration.GetValue("Logging:RetentionDays", 15));
                        Log.Logger = new LoggerConfiguration()
                                .MinimumLevel.Debug()
                                .Enrich.FromLogContext()
                                .WriteTo.File(Path.Combine(logPath, "HistoryExportCmd_#.log"), outputTemplate: logTemplate, rollingInterval: RollingInterval.Day, retainedFileCountLimit: retainedFiles)
                                .CreateLogger();
                        this._log = Log.Logger.ForContext<Program>();
                        this._log.Information("Starting");

                        this._processingSettings = new ProcessingSettings
                        {
                                OldestDayFromToday = Math.Max(0, configuration.GetValue("Processing:OldestDayFromToday", 1295)),
                                PointBatchSize = Math.Max(1, configuration.GetValue("Processing:PointBatchSize", 10))
                        };

                        this._redundantPointHistory = configuration.GetValue("Redundancy:Enabled", false);
                }

                public static async Task<int> Main(string[] args)
                {
                        string baseDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? Directory.GetCurrentDirectory();
                        string environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production";

                        IConfigurationRoot configuration = new ConfigurationBuilder()
                                .SetBasePath(baseDirectory)
                                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
                                .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: false)
                                .AddEnvironmentVariables()
                                .Build();

                        using Program program = new Program(configuration);
                        return await program.DoWorkAsync().ConfigureAwait(false);
                }

                public void Dispose()
                {
                        this._log.Information("Finished");
                        Log.CloseAndFlush();
                }

                private async Task<int> DoWorkAsync()
                {
                        string cnPHistory = this._configuration.GetConnectionString("PointsHistory");
                        string cnEbiOdbc = this._configuration.GetConnectionString("EBI_ODBC");
                        string cnEbiSql = this._configuration.GetConnectionString("EBI_SQL");

                        if (string.IsNullOrWhiteSpace(cnPHistory) || string.IsNullOrWhiteSpace(cnEbiOdbc) || string.IsNullOrWhiteSpace(cnEbiSql))
                        {
                                this._log.Error("One or more connection strings are missing from the configuration.");
                                return 1;
                        }

                        DBAccess dbaccess = new DBAccess(this._log, cnEbiSql, cnEbiOdbc, cnPHistory);
                        var (statusSuccess, primary) = await dbaccess.GetEBIStatusAsync().ConfigureAwait(false);

                        if (!statusSuccess)
                        {
                                this._log.Information("Failure reading the EBI status, the process cannot run");
                                return 1;
                        }

                        if (primary)
                        {
                                await this.ProcessAsync(dbaccess).ConfigureAwait(false);
                        }
                        else
                        {
                                await this.SynchronizeAsync().ConfigureAwait(false);
                        }

                        return 0;
                }

                private async Task ProcessAsync(DBAccess dbaccess)
                {
                        this._log.Information("Starting the process");
                        DateTime now = DateTime.UtcNow;
                        DateTime oldestDay = now.Date.AddDays(-this._processingSettings.OldestDayFromToday);

                        var (pointsSuccess, points) = await dbaccess.GetPointsAsync().ConfigureAwait(false);
                        if (!pointsSuccess)
                        {
                                this._log.Information("Process finished");
                                return;
                        }

                        this._log.Information("{PointCount} points read from database", points.Count);

                        foreach (HistoryType historyType in new[] { HistoryType.Fast, HistoryType.Slow, HistoryType.Extended })
                        {
                                this._log.Information("Working on {HistoryType}", historyType.GetDisplayName());
                                List<Point> filteredPoints = FilterPoints(points, historyType);
                                this._log.Information("{PointCount} points configured for this History type", filteredPoints.Count);
                                if (filteredPoints.Count == 0)
                                {
                                        continue;
                                }

                                var (lastSuccess, lastDatetime) = await dbaccess.GetLastDatetimeAsync(historyType).ConfigureAwait(false);
                                if (!lastSuccess)
                                {
                                        continue;
                                }

                                DateTime iniDatetime = lastDatetime ?? oldestDay;
                                if (iniDatetime < oldestDay)
                                {
                                        iniDatetime = oldestDay;
                                }
                                else
                                {
                                        iniDatetime = iniDatetime.AddSeconds(3);
                                }

                                DateTime limit = now.AddMinutes(-130.0);
                                bool res = true;
                                while (iniDatetime < limit && res)
                                {
                                        DateTime endDatetime = iniDatetime.AddSeconds(3200 * historyType.GetIntervalSeconds());
                                        if (endDatetime > limit)
                                        {
                                                endDatetime = limit;
                                        }

                                        if (endDatetime - iniDatetime > TimeSpan.FromSeconds(historyType.GetIntervalSeconds()))
                                        {
                                                res = await dbaccess.PrepareAsync().ConfigureAwait(false);
                                                if (res)
                                                {
                                                        int index = 0;
                                                        while (index < filteredPoints.Count && res)
                                                        {
                                                                int count = Math.Min(this._processingSettings.PointBatchSize, filteredPoints.Count - index);
                                                                List<Point> batch = filteredPoints.GetRange(index, count);
                                                                var (historySuccess, history) = await dbaccess.GetHistoryAsync(historyType, iniDatetime, endDatetime, batch).ConfigureAwait(false);
                                                                if (historySuccess)
                                                                {
                                                                        res = await dbaccess.StoreHistoryAsync(historyType, history).ConfigureAwait(false);
                                                                }
                                                                else
                                                                {
                                                                        res = false;
                                                                }
                                                                index += count;
                                                        }
                                                        if (res)
                                                        {
                                                                res = await dbaccess.FinishAsync(historyType).ConfigureAwait(false);
                                                        }
                                                        else
                                                        {
                                                                dbaccess.Reset();
                                                        }
                                                }
                                        }

                                        iniDatetime = endDatetime;
                                }
                        }

                        this._log.Information("Process finished");
                }

                private async Task SynchronizeAsync()
                {
                        if (!this._redundantPointHistory)
                        {
                                return;
                        }

                        string backupServer = Environment.MachineName;
                        string primaryServer;
                        if (backupServer.EndsWith("A", StringComparison.OrdinalIgnoreCase))
                        {
                                primaryServer = backupServer.Substring(0, backupServer.Length - 1) + "B";
                        }
                        else
                        {
                                primaryServer = backupServer.Substring(0, backupServer.Length - 1) + "A";
                        }

                        string cnPHistory = this._configuration.GetConnectionString("PointsHistory");
                        if (string.IsNullOrWhiteSpace(cnPHistory))
                        {
                                this._log.Error("PointsHistory connection string is missing; synchronization cannot continue.");
                                return;
                        }

                        this._log.Information("Starting the process");
                        DBSync dbaccess = new DBSync(this._log, cnPHistory);
                        bool res = await dbaccess.SyncPointAsync(primaryServer, backupServer).ConfigureAwait(false);
                        foreach (HistoryType historyType in new[] { HistoryType.Fast, HistoryType.Slow, HistoryType.Extended })
                        {
                                if (!res)
                                {
                                        break;
                                }
                                res = await dbaccess.SyncHistoryTableAsync(primaryServer, backupServer, historyType).ConfigureAwait(false);
                        }
                        this._log.Information("Process finished");
                }

                private static List<Point> FilterPoints(IReadOnlyList<Point> points, HistoryType historyType)
                {
                        switch (historyType)
                        {
                                case HistoryType.Fast:
                                        return points.Where(p => p.HistoryFast && p.HistoryFastArch).ToList();
                                case HistoryType.Slow:
                                        return points.Where(p => p.HistorySlow && p.HistorySlowArch).ToList();
                                case HistoryType.Extended:
                                        return points.Where(p => p.HistoryExtd && p.HistoryExtdArch).ToList();
                                default:
                                        throw new ArgumentOutOfRangeException(nameof(historyType), historyType, null);
                        }
                }

                private readonly IConfiguration _configuration;

                private readonly ProcessingSettings _processingSettings;

                private readonly bool _redundantPointHistory;

                private readonly Serilog.ILogger _log;

                private sealed class ProcessingSettings
                {
                        public int OldestDayFromToday { get; init; }

                        public int PointBatchSize { get; init; }
                }
        }
}
