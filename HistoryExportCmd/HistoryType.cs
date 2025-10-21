using System;

namespace HistoryExportCmd
{
        internal enum HistoryType
        {
                Fast = 1,
                Slow = 2,
                Extended = 3
        }

        internal static class HistoryTypeExtensions
        {
                public static string GetDisplayName(this HistoryType type)
                {
                        switch (type)
                        {
                                case HistoryType.Fast:
                                        return "Fast History";
                                case HistoryType.Slow:
                                        return "Slow History";
                                case HistoryType.Extended:
                                        return "Extended History";
                                default:
                                        throw new ArgumentOutOfRangeException(nameof(type), type, null);
                        }
                }

                public static int GetIntervalSeconds(this HistoryType type)
                {
                        switch (type)
                        {
                                case HistoryType.Fast:
                                        return 5;
                                case HistoryType.Slow:
                                        return 60;
                                case HistoryType.Extended:
                                        return 3600;
                                default:
                                        throw new ArgumentOutOfRangeException(nameof(type), type, null);
                        }
                }
        }
}
