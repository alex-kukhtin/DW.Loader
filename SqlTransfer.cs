using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace DW.Loader
{
    internal class SqlTransfer
    {
        String _sourceConnStr;
        String _targetConnStr;
        String _sourceProcedure;
        String _targetProcedure;
        String _key;
        public SqlTransfer(JobSettings job, String connSrc, String connTrg)
        {
            _sourceConnStr = connSrc;
            _targetConnStr = connTrg;
            _sourceProcedure = job.SourceProcedurePrefix;
            _targetProcedure = job.TargetProcedurePrefix;
            _key = job.DataKey;
        }

        public void Run()
        {
            while (RunOne())
                ;
        }

        public void RunStart()
        {
            RunOne();
        }

        bool RunOne()
        {
            int count = 0;
            DateTime startTime = DateTime.Now;
            DateTime jobDate = new DateTime(1900, 1, 1);
            Byte[] jobTimeStamp = null;
            try
            {
                using (var cnns = new SqlConnection(_sourceConnStr))
                using (var cnnd = new SqlConnection(_targetConnStr))
                {
                    cnns.Open();
                    cnnd.Open();
                    using (var cmdtime = cnns.CreateCommand())
                    {
                        cmdtime.CommandType = CommandType.StoredProcedure;
                        cmdtime.CommandText = _sourceProcedure + "_gettime";
                        cmdtime.CommandTimeout = Global.CommandTimeout;
                        cmdtime.Parameters.AddWithValue("@key", _key);
                        using (var rdr = cmdtime.ExecuteReader(CommandBehavior.SingleRow))
                        {
                            if (rdr.Read())
                            {
                                jobDate = rdr.GetDateTime(0);
                                jobTimeStamp = (Byte[])rdr.GetValue(1);
                            }
                            else
                            {
                                Console.WriteLine("\tНет актуальных данных");
                                return false;
                            }
                        }

                    }
                    Console.Write("\tОбработка даты: {0}", jobDate.ToShortDateString());
                    // Запускаем обработку
                    using (var scmd = cnnd.CreateCommand())
                    {
                        scmd.CommandType = CommandType.StoredProcedure;
                        scmd.CommandText = _targetProcedure + "_start";
                        scmd.CommandTimeout = Global.CommandTimeout;
                        scmd.Parameters.AddWithValue("@date", jobDate);
                        scmd.ExecuteNonQuery();
                    }
                    // Передача данных
                    using (var scmd = cnns.CreateCommand())
                    using (var tcmd = cnnd.CreateCommand())
                    {
                        scmd.CommandType = CommandType.StoredProcedure;
                        scmd.CommandText = _sourceProcedure + "_load";
                        scmd.CommandTimeout = Global.CommandTimeout;
                        tcmd.CommandType = CommandType.StoredProcedure;
                        tcmd.CommandText = _targetProcedure + "_write";
                        tcmd.CommandTimeout = Global.CommandTimeout;
                        scmd.Parameters.AddWithValue("@date", jobDate);
                        using (var rdr = scmd.ExecuteReader())
                        {
                            int fc = rdr.FieldCount;
                            for (int i = 0; i < fc; i++)
                            {
                                SqlDbType dbt = (SqlDbType) Enum.Parse(typeof(SqlDbType), rdr.GetDataTypeName(i), true);
                                tcmd.Parameters.Add(new SqlParameter("@" + rdr.GetName(i), dbt));
                            }
                            while (rdr.Read())
                            {
                                for (int i=0; i<fc; i++)
                                {
                                    tcmd.Parameters[i].Value = rdr.GetValue(i);
                                }
                                tcmd.ExecuteNonQuery();
                                count++;
                            }
                        }
                        // завершение обработки
                        using (var cmdx = cnnd.CreateCommand())
                        {
                            // destination
                            cmdx.CommandType = CommandType.StoredProcedure;
                            cmdx.CommandText = _targetProcedure + "_written";
                            cmdx.CommandTimeout = Global.CommandTimeout;
                            cmdx.Parameters.AddWithValue("@date", jobDate);
                            cmdx.ExecuteNonQuery();
                        }
                        using (var cmdd = cnns.CreateCommand())
                        {
                            // source
                            cmdd.CommandType = CommandType.StoredProcedure;
                            cmdd.CommandText = _sourceProcedure + "_loaded";
                            cmdd.CommandTimeout = Global.CommandTimeout;
                            cmdd.Parameters.AddWithValue("@key", _key);
                            cmdd.Parameters.AddWithValue("@date", jobDate);
                            cmdd.Parameters.AddWithValue("@timestamp", jobTimeStamp);
                            cmdd.ExecuteNonQuery();
                        }
                        int seconds = Convert.ToInt32((DateTime.Now - startTime).TotalSeconds);
                        Console.WriteLine(" ({0} строк за {1} сек.)", count.ToString(), seconds.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                if (ex.InnerException != null)
                    Console.WriteLine(ex.InnerException.Message);
                Console.WriteLine(ex.Message);
                Program.IsRestarting = true;
                return false;
            }
            return true;
        }
    }
}
