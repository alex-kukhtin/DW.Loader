using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

namespace DW.Loader
{
    internal class SqlCatalogTransfer
    {
        String _sourceConnStr;
        String _targetConnStr;
        String _sourceProcedure;
        String _targetProcedure;
        String _key;
        public SqlCatalogTransfer(JobSettings job, String connSrc, String connTrg)
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
        Boolean RunOne()
        {
            int count = 0;
            DateTime startTime = DateTime.Now;
            DateTime jobDate = new DateTime(1900, 1, 1);
            Int64 pkgId = 0;
            try
            {
                using (var cnns = new SqlConnection(_sourceConnStr))
                using (var cnnd = new SqlConnection(_targetConnStr))
                {
                    cnns.Open();
                    cnnd.Open();
                    using (var cmd = cnnd.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = _targetProcedure + "_getpkgid";
                        cmd.CommandTimeout = Global.CommandTimeout; 
                        cmd.Parameters.AddWithValue("@retid", (Int64)0).Direction = ParameterDirection.Output;
                        cmd.ExecuteNonQuery();
                        pkgId = (Int64) cmd.Parameters["@retid"].Value;
                        Console.WriteLine("ID последнего загруженного пакета id={0}", pkgId.ToString());
                    }
                    pkgId++;
                    Console.WriteLine("Загрузка пакета id={0}", pkgId.ToString());
                    using (var cmd = cnns.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = _sourceProcedure + "_load";
                        cmd.CommandTimeout = Global.CommandTimeout;
                        cmd.Parameters.AddWithValue("@pkgid", pkgId);
                        using (var rdr = cmd.ExecuteReader())
                        {
                            do
                            {
                                using (var tcmd = cnnd.CreateCommand())
                                {
                                    tcmd.CommandType = CommandType.StoredProcedure;
                                    int fc = rdr.FieldCount;
                                    for (int i = 0; i < fc; i++)
                                    {
                                        SqlDbType dbt = (SqlDbType)Enum.Parse(typeof(SqlDbType), rdr.GetDataTypeName(i), true);
                                        tcmd.Parameters.Add(new SqlParameter("@" + rdr.GetName(i), dbt));
                                    }
                                    while (rdr.Read())
                                    {
                                        String tableName = rdr.GetString(0);
                                        if (tableName == "EMPTYPACKAGE")
                                            count = 1;
                                        else
                                        {
                                            if (String.IsNullOrEmpty(tcmd.CommandText))
                                                tcmd.CommandText = _targetProcedure + "_" + tableName + "_write";
                                            for (int i = 0; i < fc; i++)
                                            {
                                                tcmd.Parameters[i].Value = rdr.GetValue(i);
                                            }
                                            tcmd.ExecuteNonQuery();
                                            count++;
                                        }
                                    }
                                }

                            } while (rdr.NextResult());
                        }
                        if (count != 0)
                        {
                            using (var cmdx = cnnd.CreateCommand())
                            {
                                cmdx.CommandType = CommandType.StoredProcedure;
                                cmdx.CommandText = _targetProcedure + "_setpkgid";
                                cmdx.CommandTimeout = Global.CommandTimeout; 
                                cmdx.Parameters.AddWithValue("@pkgid", pkgId);
                                cmdx.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            Console.WriteLine("Нет актуальных данных");
                            return false; // Завершили
                        }
                    }
                }
                int seconds = Convert.ToInt32((DateTime.Now - startTime).TotalSeconds);
                Console.WriteLine(" ({0} записей за {1} сек.)", count.ToString(), seconds.ToString());
            }
            catch (SqlException ex)
            {
                Console.WriteLine();
                if (ex.InnerException != null)
                    Console.WriteLine(ex.InnerException.Message);
                Console.WriteLine(ex.Message);
                Program.IsRestarting = true;
                return false;
            }
            return true; // продолжаем
        }
    }
}
