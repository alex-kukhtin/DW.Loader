using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

namespace DW.Loader
{
    internal class SqlDocumentTransfer
    {
        String _sourceConnStr;
        String _targetConnStr;
        String _sourceProcedure;
        String _targetProcedure;
        String _key;
        Int32 _count;
        Boolean _useGuid;

        public SqlDocumentTransfer(JobSettings job, String connSrc, String connTrg)
        {
            _sourceConnStr = connSrc;
            _targetConnStr = connTrg;
            _sourceProcedure = job.SourceProcedurePrefix;
            _targetProcedure = job.TargetProcedurePrefix;
            _key = job.DataKey;
            _useGuid = job.UseGuid;
        }

        public void Run()
        {
            DateTime startTime = DateTime.Now;
            _count = 0;
            while (RunOne())
                ;
            if (_count == 0)
            {
                Console.WriteLine("Нет актуальных данных");
                return;
            }
            int seconds = Convert.ToInt32((DateTime.Now - startTime).TotalSeconds);
            Console.WriteLine(" ({0} документов за {1} сек.)", _count.ToString(), seconds.ToString());
        }

        Boolean RunOne()
        {
            DateTime jobDate = new DateTime(1900, 1, 1);
            Int64 docId = 0;
            Guid docGuid = Guid.Empty;
            try
            {
                using (var cnns = new SqlConnection(_sourceConnStr))
                using (var cnnd = new SqlConnection(_targetConnStr))
                {
                    cnns.Open();
                    cnnd.Open();
                    using (var cmd = cnns.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = Global.CommandTimeout;
                        if (_useGuid)
                        {
                            cmd.CommandText = _targetProcedure + "_getguid";
                            cmd.Parameters.AddWithValue("@key", _key);
                            cmd.Parameters.AddWithValue("@retguid", Guid.Empty).Direction = ParameterDirection.Output;
                            cmd.ExecuteNonQuery();
                            var val = cmd.Parameters["@retguid"].Value;
                            if (val != DBNull.Value)
                                docGuid = (Guid) val;
                            else
                            {
                                docGuid = Guid.Empty;
                                return false;
                            }
                        }
                        else
                        {
                            cmd.CommandText = _targetProcedure + "_getid";
                            cmd.Parameters.AddWithValue("@key", _key);
                            cmd.Parameters.AddWithValue("@retid", (Int64)0).Direction = ParameterDirection.Output;
                            cmd.ExecuteNonQuery();
                            var val = cmd.Parameters["@retid"].Value;
                            if (val != DBNull.Value)
                                docId = (Int64) val;
                            else
                            {
                                docId = 0;
                                return false;
                            }
                        }
                    }
                    if ((docId != 0) || (docGuid != Guid.Empty))
                    {
                        _count++;
                        if (_useGuid)
                            Console.WriteLine("  Загрузка документа (Guid={0})", docGuid.ToString());
                        else
                            Console.WriteLine("  Загрузка документа (Id={0})", docId.ToString());
                        using (var cmd = cnns.CreateCommand())
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandText = _sourceProcedure + "_load";
                            cmd.CommandTimeout = Global.CommandTimeout;
                            cmd.Parameters.AddWithValue("@key", _key);
                            if (_useGuid)
                                cmd.Parameters.AddWithValue("@docguid", docGuid);
                            else
                                cmd.Parameters.AddWithValue("@docid", docId);
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
                                            if (String.IsNullOrEmpty(tcmd.CommandText))
                                                tcmd.CommandText = _targetProcedure + "_" + tableName + "_write";
                                            for (int i = 0; i < fc; i++)
                                            {
                                                tcmd.Parameters[i].Value = rdr.GetValue(i);
                                            }
                                            tcmd.ExecuteNonQuery();
                                        }
                                    }

                                } while (rdr.NextResult());
                            }
                        }
                        using (var cmdx = cnns.CreateCommand())
                        {
                            cmdx.CommandType = CommandType.StoredProcedure;
                            cmdx.CommandText = _targetProcedure + "_written";
                            cmdx.CommandTimeout = Global.CommandTimeout;
                            cmdx.Parameters.AddWithValue("@key", _key);
                            if (_useGuid)
                                cmdx.Parameters.AddWithValue("@docguid", docGuid);
                            else
                                cmdx.Parameters.AddWithValue("@docid", docId);
                            cmdx.ExecuteNonQuery();
                        }
                    }
                }
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
