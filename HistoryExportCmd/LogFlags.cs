using System;

namespace HistoryExportCmd
{
	// Token: 0x02000005 RID: 5
	public enum LogFlags : uint
	{
		// Token: 0x0400000A RID: 10
		TzEXCEPTION = 1U,
		// Token: 0x0400000B RID: 11
		TzINFORMATION,
		// Token: 0x0400000C RID: 12
		TzSQL = 4U,
		// Token: 0x0400000D RID: 13
		TzERROR = 16U,
		// Token: 0x0400000E RID: 14
		TzDEBUG = 64U
	}
}
