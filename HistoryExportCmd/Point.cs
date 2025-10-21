using System;

namespace HistoryExportCmd
{
	// Token: 0x02000008 RID: 8
	internal class Point
	{
		// Token: 0x17000001 RID: 1
		// (get) Token: 0x06000026 RID: 38 RVA: 0x0000402D File Offset: 0x0000222D
		// (set) Token: 0x06000027 RID: 39 RVA: 0x00004035 File Offset: 0x00002235
		public int PointId { get; set; }

		// Token: 0x17000002 RID: 2
		// (get) Token: 0x06000028 RID: 40 RVA: 0x0000403E File Offset: 0x0000223E
		// (set) Token: 0x06000029 RID: 41 RVA: 0x00004046 File Offset: 0x00002246
		public string PointName { get; set; }

		// Token: 0x17000003 RID: 3
		// (get) Token: 0x0600002A RID: 42 RVA: 0x0000404F File Offset: 0x0000224F
		// (set) Token: 0x0600002B RID: 43 RVA: 0x00004057 File Offset: 0x00002257
		public string ParamName { get; set; }

		// Token: 0x17000004 RID: 4
		// (get) Token: 0x0600002C RID: 44 RVA: 0x00004060 File Offset: 0x00002260
		// (set) Token: 0x0600002D RID: 45 RVA: 0x00004068 File Offset: 0x00002268
		public string Descriptor { get; set; }

		// Token: 0x17000005 RID: 5
		// (get) Token: 0x0600002E RID: 46 RVA: 0x00004071 File Offset: 0x00002271
		// (set) Token: 0x0600002F RID: 47 RVA: 0x00004079 File Offset: 0x00002279
		public string Device { get; set; }

		// Token: 0x17000006 RID: 6
		// (get) Token: 0x06000030 RID: 48 RVA: 0x00004082 File Offset: 0x00002282
		// (set) Token: 0x06000031 RID: 49 RVA: 0x0000408A File Offset: 0x0000228A
		public bool HistoryFast { get; set; }

		// Token: 0x17000007 RID: 7
		// (get) Token: 0x06000032 RID: 50 RVA: 0x00004093 File Offset: 0x00002293
		// (set) Token: 0x06000033 RID: 51 RVA: 0x0000409B File Offset: 0x0000229B
		public bool HistorySlow { get; set; }

		// Token: 0x17000008 RID: 8
		// (get) Token: 0x06000034 RID: 52 RVA: 0x000040A4 File Offset: 0x000022A4
		// (set) Token: 0x06000035 RID: 53 RVA: 0x000040AC File Offset: 0x000022AC
		public bool HistoryExtd { get; set; }

		// Token: 0x17000009 RID: 9
		// (get) Token: 0x06000036 RID: 54 RVA: 0x000040B5 File Offset: 0x000022B5
		// (set) Token: 0x06000037 RID: 55 RVA: 0x000040BD File Offset: 0x000022BD
		public bool HistoryFastArch { get; set; }

		// Token: 0x1700000A RID: 10
		// (get) Token: 0x06000038 RID: 56 RVA: 0x000040C6 File Offset: 0x000022C6
		// (set) Token: 0x06000039 RID: 57 RVA: 0x000040CE File Offset: 0x000022CE
		public bool HistorySlowArch { get; set; }

		// Token: 0x1700000B RID: 11
		// (get) Token: 0x0600003A RID: 58 RVA: 0x000040D7 File Offset: 0x000022D7
		// (set) Token: 0x0600003B RID: 59 RVA: 0x000040DF File Offset: 0x000022DF
		public bool HistoryExtdArch { get; set; }
	}
}
