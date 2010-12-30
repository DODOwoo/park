using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections.Specialized;
using System.IO;
using System.Configuration;
using System.Globalization;
using System.Net.Mail;
using System.Net;

namespace DP_APINUVTotal
{
    class Program
    {
        static void Main(string[] args)
        {
            ReadDataFromTXT();
            //ReadFromDBAList();
            //Console.ReadLine();
        }

        private static List<string> listFile;
        public static void ReadDataFromTXT()
        {
            DateTime startTime = DateTime.Now;

            //获取日志文件地址
            listFile = new List<string>();

            string logFold = System.Configuration.ConfigurationManager.AppSettings["logfold"];
            logFold = String.IsNullOrEmpty(logFold) ? AppDomain.CurrentDomain.BaseDirectory : logFold;
            listFile.AddRange(Directory.GetFiles(logFold, "*.txt"));

            DateTime fileDate = DateTime.Now;
            foreach (var file in listFile)
            {
                fileDate = file.GetFileDateTime().FormatDateTime().Date;
                if (fileDate >= DateTime.Now.Date) continue;

                DataTable dt = GetDeviceIDList(fileDate);
                List<NUVList> nuvlist = getNUVTotal(fileDate.AddDays(-2),fileDate.AddDays(1));

                Console.WriteLine("分析文件:" + file);
                Dictionary<string, string[]> dict = file.LoadAndManipulate<Dictionary<string, string[]>>((NameValueCollection nvc, Dictionary<string, string[]> y) =>
                    {
                        bool isNew = true;
                        string deviceID = (nvc["imei"] + string.Empty).ToLower();
                        string olddeviceID = string.Empty;
                        if (String.IsNullOrEmpty(deviceID))
                        {
                            deviceID = (nvc["deviceID"] + string.Empty).ToLower();
                        }
                        else
                        {
                            isNew = false;
                            olddeviceID = (nvc["deviceID"] + string.Empty).ToLower();
                        }
                        DataRow[] drs = dt.Select("DeviceID='" + deviceID + "'");
                        if (drs.Length == 0 && !y.ContainsKey(deviceID))
                        {
                            if (isNew)
                            {
                                y[deviceID] = new string[] { fileDate.ToShortDateString(), string.Empty, "1" };
                                NUVList auniquenuv = nuvlist.Find(delegate(NUVList anuv) { return anuv.TrainID == nvc["trainid"] + string.Empty && anuv.Source == (nvc["source"] + string.Empty).ToLower() && anuv.AddDate == fileDate; });
                                if (auniquenuv == null)
                                {
                                    nuvlist.Add(new NUVList { TrainID = nvc["trainid"] + string.Empty, Source = (nvc["source"] + string.Empty).ToLower(), AddDate = fileDate, NUVTotal = 1, changestatus = (int)ChangeType.insert });
                                }
                                else
                                {
                                    auniquenuv.NUVTotal++;
                                    if (auniquenuv.changestatus != (int)ChangeType.insert)
                                    {
                                        auniquenuv.changestatus = (int)ChangeType.update;
                                    }
                                }
                            }
                            else
                            {
                                y[deviceID] = new string[] { fileDate.ToShortDateString(), olddeviceID, "0" };
                            }                                                        
                        }

                        return y;
                    });

                Console.WriteLine("start at :" + startTime + "  && get all data end at : " + DateTime.Now);

                try
                {
                    InsertToDB(dict, startTime);
                }
                catch
                {
#if !DEBUG
                    DHelper.SendWithoutQueue("info@dianping.com", "minxuan.cheng@dianping.com", "log日志新增用户数-分析失败", file + "插入devicelist失败" + DateTime.Now);
#endif
                }
                try
                {
                    NUVListDac.insetData(nuvlist);
                }
                catch
                {
#if !DEBUG
                    DHelper.SendWithoutQueue("info@dianping.com", "minxuan.cheng@dianping.com", "log日志新增用户数-分析失败", file + "插入更新nuvlist失败" + DateTime.Now);
#endif
                }
                Console.WriteLine("start at :" + startTime + "  && nuvlist to db end at : " + DateTime.Now);

#if !DEBUG
				string backFold = ConfigurationManager.AppSettings["backfold"];
				string backPath = String.IsNullOrEmpty(backFold) ? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "back") : backFold;

				System.IO.Directory.CreateDirectory(backPath);
				System.IO.File.Move(file, Path.Combine(backPath, Path.GetFileName(file)));
#endif
            }

#if !DEBUG
            DHelper.SendWithoutQueue("info@dianping.com", "minxuan.cheng@dianping.com", "log日志新增用户数-分析完成", DateTime.Now +" 生成成功");
#endif
            Console.WriteLine("-----------end-----------");
        }

        private static DataTable GetDeviceIDList(DateTime dtCurrentDate)
        {
            string selectdevicelistsql = "select * from DP_DLogDeviceID where AddDate <= '" + dtCurrentDate + "'";
            return selectdevicelistsql.LoadDataFromDB();
        }

        private static List<NUVList> getNUVTotal(DateTime dtStartDate, DateTime dtEndDate)
        {
            string selectnuvlist = "select * from DP_DLogAnalysis_NUV where AddDate >= '" + dtStartDate + "' and AddDate < '" + dtEndDate + "' ";
            DataTable dtnuvlist = selectnuvlist.LoadDataFromDB();

            List<NUVList> nuvlist = new List<NUVList>();
            foreach (DataRow dr in dtnuvlist.Rows)
            {
                nuvlist.Add(new NUVList { TrainID = dr["TrainID"] + string.Empty, Source = dr["Source"] + string.Empty, AddDate = Convert.ToDateTime(dr["AddDate"] + string.Empty), NUVTotal = dr["NUVTotal"].ToInt(), changestatus = (int)ChangeType.nochange });
            }
            return nuvlist;
        }

        #region 处理dba导出的特定txt
        public static void ReadFromDBAList()
        {
            DateTime startTime = DateTime.Now;
            //string filename = "justapitestfile222.text";
            //int fieldcount = 2;
            //LoadFromDB(filename, fieldcount);

            string filename = "DP_DLogAnalysis_12_09.txt";
            int fieldcount = 4;
            //Dictionary<string, string> devicelist = new Dictionary<string, string>();
            string selectdevicelistsql = "select * from DP_DLogDeviceID where AddDate < '2010-12-09'";
            DataTable dt = selectdevicelistsql.LoadDataFromDB();

            //string selectnuvlist = "select * from DP_DLogAnalysis_NUV where AddDate < '2010-08-01'";
            string selectnuvlist = "select * from DP_DLogAnalysis_NUV";
            DataTable dtnuvlist = selectnuvlist.LoadDataFromDB();

            List<NUVList> nuvlist = new List<NUVList>();
            foreach (DataRow dr in dtnuvlist.Rows)
            {
                nuvlist.Add(new NUVList { TrainID = dr["TrainID"] + string.Empty, Source = dr["Source"] + string.Empty, AddDate = Convert.ToDateTime(dr["AddDate"] + string.Empty), NUVTotal = dr["NUVTotal"].ToInt(), changestatus = (int)ChangeType.nochange });
            }
            Dictionary<string, string[]> newlist = filename.LoadAndManipulate<Dictionary<string, string[]>>(fieldcount, (string[] x, Dictionary<string, string[]> y) =>
            {
                // x[0]: trainid; x[1]: source; x[2]: deviceid; x[3]: AddDate;
                string key = x[2] + string.Empty;
                DataRow[] drs = dt.Select("DeviceID='" + key + "'");
                if (drs.Length == 0 && !y.ContainsKey(key))  //if (!devicelist.ContainsKey(key))
                {
                    y[key] = new string[] { x[3] + string.Empty, "", "1" };

                    NUVList auniquenuv = nuvlist.Find(delegate(NUVList anuv) { return anuv.TrainID == x[0] + string.Empty && anuv.Source == x[1] + string.Empty && anuv.AddDate == Convert.ToDateTime(x[3] + string.Empty).Date; });
                    if (auniquenuv == null)
                    {
                        nuvlist.Add(new NUVList { TrainID = x[0] + string.Empty, Source = x[1] + string.Empty, AddDate = Convert.ToDateTime(x[3] + string.Empty).Date, NUVTotal = 1, changestatus = (int)ChangeType.insert });
                    }
                    else
                    {
                        auniquenuv.NUVTotal++;
                        if (auniquenuv.changestatus != (int)ChangeType.insert)
                        {
                            auniquenuv.changestatus = (int)ChangeType.update;
                        }
                    }
                }
                return y;
            });

            Console.WriteLine("start at :" + startTime + "  && get all data end at : " + DateTime.Now);

            //InsertToDB(newlist, startTime);

            NUVListDac.insetData(nuvlist);
            Console.WriteLine("start at :" + startTime + "  && nuvlist to db end at : " + DateTime.Now);

            Console.WriteLine("-----------end-----------");
        }
        #endregion

        private static void InsertToDB(Dictionary<string, string[]> newlist, DateTime startTime)
        {
            string sqlinsert = "insert into DP_DLogDeviceID values ";
            newlist.SaveToDB("", (string sx, string[] sy) =>
            {
                string restsql = "'" + sy[0] + "','" + sy[1] + "'," + sy[2];

                return sqlinsert + "('" + sx + "'," + restsql + ")";
            });

            Console.WriteLine("start at :" + startTime + "  && deviceIDlist to db end at : " + DateTime.Now);
        }
    }

    static class ToFunc
    {
        public static int ToInt(this string source)
        {
            int result = 0;
            try
            {
                result = int.Parse(source);
            }
            catch
            {
                result = 100;
            }
            return result;
        }

        public static string GetFileDateTime(this string path)
        {
            string fileName = Path.GetFileNameWithoutExtension(path);
            int sIndex = fileName.IndexOf('(') + 1;
            int eIndex = fileName.IndexOf(')');
            return fileName.Substring(sIndex, eIndex - sIndex);

        }

        public static DateTime FormatDateTime(this string dateTime)
        {
            long resulte = 0;

            DateTime dt;

            if (long.TryParse(dateTime, out resulte))
            {

                dt = new DateTime(1970, 1, 1, 0, 0, 0, 0, new GregorianCalendar(), DateTimeKind.Utc);
                dt = dt.AddMilliseconds(resulte + 8 * 3600000);
                return dt;
            }


            if (!DateTime.TryParse(dateTime, out dt))
            {
                dt = DateTime.Now;
            }
            return dt;
        }
    }

    
}
