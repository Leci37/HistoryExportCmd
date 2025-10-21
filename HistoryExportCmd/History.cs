using System;

namespace HistoryExportCmd
{
	// Token: 0x02000009 RID: 9
        internal class History
        {
                public int PointId { get; set; }

                public DateTime Timestamp { get; set; }

                public DateTime USTTimestamp { get; set; }

                public double Value { get; set; }
        }
}
