using System;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace HistoryExportCmd
{
	// Token: 0x02000006 RID: 6
	public class LogFile
	{
		// Token: 0x06000014 RID: 20 RVA: 0x00003514 File Offset: 0x00001714
		public static void SetConfig(Type flags, NameValueCollection config)
		{
			LogFile.m_sMask = LogFile.GetMask(flags, config);
			if (config["LogPath"] != null)
			{
				LogFile.m_sLogPath = Convert.ToString(config["LogPath"]);
			}
			uint val;
			if (config["LogMaxDays"] != null && uint.TryParse(config["LogMaxDays"], out val))
			{
				LogFile.m_sMaxDays = val;
			}
		}

		// Token: 0x06000015 RID: 21 RVA: 0x00003576 File Offset: 0x00001776
		public static void SetLogPath(string logPath)
		{
			LogFile.m_sLogPath = logPath;
		}

		// Token: 0x06000016 RID: 22 RVA: 0x00003580 File Offset: 0x00001780
		public LogFile(string Filename)
		{
			this.m_Lock = new object();
			if (Path.IsPathRooted(Filename))
			{
				this.m_Filename = Filename;
			}
			else
			{
				this.m_Filename = LogFile.m_sLogPath + "\\" + Filename;
			}
			this.m_CurrentDay = 0;
			this.m_Mask = LogFile.m_sMask;
			this.m_MaxDays = LogFile.m_sMaxDays;
		}

		// Token: 0x06000017 RID: 23 RVA: 0x000035E2 File Offset: 0x000017E2
		public LogFile(string Filename, uint logMaxDays, Type flags, NameValueCollection config)
		{
			this.m_Lock = new object();
			this.m_Filename = Filename;
			this.m_MaxDays = logMaxDays;
			this.m_CurrentDay = 0;
			this.m_Mask = LogFile.GetMask(flags, config);
		}

		// Token: 0x06000018 RID: 24 RVA: 0x00003618 File Offset: 0x00001818
		public void Write(Exception ex)
		{
			this.Write(LogFlags.TzEXCEPTION, ex.ToString());
		}

		// Token: 0x06000019 RID: 25 RVA: 0x0000362C File Offset: 0x0000182C
		public void Write(Enum flag, string text, params object[] pars)
		{
			string aux = string.Format(text, pars);
			this.Write(flag, aux);
		}

		// Token: 0x0600001A RID: 26 RVA: 0x00003649 File Offset: 0x00001849
		public void Write(Enum flag, string text)
		{
			if ((Convert.ToUInt32(flag) & this.m_Mask) != 0U)
			{
				this.Write(text);
			}
		}

		// Token: 0x0600001B RID: 27 RVA: 0x00003664 File Offset: 0x00001864
		public void Write(Enum flag, SqlCommand cmd)
		{
			if ((Convert.ToUInt32(flag) & this.m_Mask) != 0U)
			{
				string text = "\n";
				foreach (object obj in cmd.Parameters)
				{
					SqlParameter param = (SqlParameter)obj;
					if (param.SqlValue == null)
					{
						text += string.Format("declare {0} {1}; set {0} = NULL;\n", param.ParameterName, param.SqlDbType);
					}
					else if (param.SqlDbType.Equals(SqlDbType.DateTime))
					{
						text += string.Format("declare {0} {1}; set {0} = '{2:yyyy/MM/dd HH:mm:ss}';\n", param.ParameterName, param.SqlDbType, param.Value);
					}
					else if (param.SqlDbType.Equals(SqlDbType.NVarChar))
					{
						text += string.Format("declare {0} {1}({3}); set {0} = '{2}';\n", new object[]
						{
							param.ParameterName,
							param.SqlDbType,
							param.SqlValue,
							param.Value.ToString().Length + 1
						});
					}
					else if (param.SqlDbType.Equals(SqlDbType.Bit))
					{
						text += string.Format("declare {0} {1}; set {0} = {2};\n", param.ParameterName, param.SqlDbType, ((bool)param.Value) ? 1 : 0);
					}
					else
					{
						text += string.Format("declare {0} {1}; set {0} = {2};\n", param.ParameterName, param.SqlDbType, param.SqlValue);
					}
				}
				text += cmd.CommandText;
				this.Write(text);
			}
		}

		// Token: 0x0600001C RID: 28 RVA: 0x00003860 File Offset: 0x00001A60
		public void Write(string text)
		{
			object @lock = this.m_Lock;
			lock (@lock)
			{
				try
				{
					DateTime now = DateTime.Now;
					if (now.Day != this.m_CurrentDay)
					{
						this.RemoveOldFiles();
						string date = now.ToString("yyyyMMdd");
						this.m_CurrentFilename = this.m_Filename.Replace("#", date);
						this.m_CurrentDay = now.Day;
					}
					string textToWrite = now.ToString("G") + " " + text;
					StreamWriter streamWriter = File.AppendText(this.m_CurrentFilename);
					streamWriter.WriteLine(textToWrite);
					streamWriter.Close();
				}
				catch
				{
				}
			}
		}

		// Token: 0x0600001D RID: 29 RVA: 0x00003928 File Offset: 0x00001B28
		private void RemoveOldFiles()
		{
			string date = DateTime.Now.AddDays((double)(-(double)((ulong)this.m_MaxDays))).ToString("yyyyMMdd");
			string filename = Path.GetFileName(this.m_Filename.Replace("#", date));
			foreach (FileInfo f in new DirectoryInfo(Path.GetDirectoryName(this.m_Filename)).GetFiles(Path.GetFileName(this.m_Filename.Replace("#", "*"))))
			{
				if (f.Name.CompareTo(filename) < 0)
				{
					f.Delete();
				}
			}
		}

		// Token: 0x0600001E RID: 30 RVA: 0x000039D0 File Offset: 0x00001BD0
		private static uint GetMask(Type flags, NameValueCollection config)
		{
			uint mask = 0U;
			foreach (string flagName in Enum.GetNames(flags))
			{
				uint conf = 1U;
				int x = Convert.ToInt32(Math.Round(Math.Log(Convert.ToUInt32(Enum.Parse(flags, flagName))) / Math.Log(2.0)));
				if (config[flagName] != null && uint.TryParse(config[flagName], out conf) && conf != 0U)
				{
					conf = 1U;
				}
				mask |= conf << x;
			}
			return mask;
		}

		// Token: 0x0400000F RID: 15
		private static string m_sLogPath = ".";

		// Token: 0x04000010 RID: 16
		private static uint m_sMaxDays = 15U;

		// Token: 0x04000011 RID: 17
		private static uint m_sMask = uint.MaxValue;

		// Token: 0x04000012 RID: 18
		private uint m_MaxDays;

		// Token: 0x04000013 RID: 19
		private uint m_Mask;

		// Token: 0x04000014 RID: 20
		private object m_Lock;

		// Token: 0x04000015 RID: 21
		private string m_Filename;

		// Token: 0x04000016 RID: 22
		private string m_CurrentFilename;

		// Token: 0x04000017 RID: 23
		private int m_CurrentDay;
	}
}
