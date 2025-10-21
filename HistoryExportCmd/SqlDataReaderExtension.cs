using System;
using System.Data.SqlClient;

namespace HistoryExportCmd
{
	// Token: 0x02000003 RID: 3
	internal static class SqlDataReaderExtension
	{
		// Token: 0x0600000B RID: 11 RVA: 0x00002FAC File Offset: 0x000011AC
		public static bool? GetNullableBoolean(this SqlDataReader dr, int n)
		{
			if (!dr.IsDBNull(n))
			{
				return new bool?(dr.GetBoolean(n));
			}
			return null;
		}

		// Token: 0x0600000C RID: 12 RVA: 0x00002FD8 File Offset: 0x000011D8
		public static DateTime? GetNullableDateTime(this SqlDataReader dr, int n)
		{
			if (!dr.IsDBNull(n))
			{
				return new DateTime?(dr.GetDateTime(n));
			}
			return null;
		}

		// Token: 0x0600000D RID: 13 RVA: 0x00003004 File Offset: 0x00001204
		public static double? GetNullableDouble(this SqlDataReader dr, int n)
		{
			if (!dr.IsDBNull(n))
			{
				return new double?(dr.GetDouble(n));
			}
			return null;
		}

		// Token: 0x0600000E RID: 14 RVA: 0x00003030 File Offset: 0x00001230
		public static int? GetNullableInt32(this SqlDataReader dr, int n)
		{
			if (!dr.IsDBNull(n))
			{
				return new int?(dr.GetInt32(n));
			}
			return null;
		}

		// Token: 0x0600000F RID: 15 RVA: 0x0000305C File Offset: 0x0000125C
		public static long? GetNullableInt64(this SqlDataReader dr, int n)
		{
			if (!dr.IsDBNull(n))
			{
				return new long?(dr.GetInt64(n));
			}
			return null;
		}

		// Token: 0x06000010 RID: 16 RVA: 0x00003088 File Offset: 0x00001288
		public static string GetNullableString(this SqlDataReader dr, int n)
		{
			if (!dr.IsDBNull(n))
			{
				return dr.GetString(n);
			}
			return null;
		}
	}
}
