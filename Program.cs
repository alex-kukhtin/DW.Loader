using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Diagnostics;
using System.Reflection;

namespace DW.Loader
{
    public static class Global
    {
        public static int CommandTimeout { get { return 60 * 10; } } // 5 min
    }

    class Program
    {
        public static void WriteDivider()
        {
            Console.WriteLine("----------------");
        }

        public static Boolean IsRestarting { get; set; }
        
        static void Restart()
        {
            if (!IsRestarting)
                return;
            var c = ConfigurationManager.AppSettings["autorestart"];
            if (c == null)
                return;
            if (c.ToString() != "true")
                return;

            WriteDivider();
            Console.WriteLine("Перезапуск программы...");
            var x = System.Environment.CurrentDirectory + "\\dw.loader.exe";
            Process.Start(x);
        }

        static void Main(string[] args)
        {
            try
            {
                AssemblyName an = new AssemblyName(Assembly.GetExecutingAssembly().FullName);
                Console.WriteLine("DW.Loader [Версия: {0}.{1}.{2}]", an.Version.Major, an.Version.Minor, an.Version.Build);
                Console.WriteLine("Copyright (c) 2011,2012 А. Кухтин. Все права защищены");
                WriteDivider();
                Console.WriteLine("Программа запущена {0}", DateTime.Now.ToLongTimeString());
                Jobs jobsSection = ConfigurationManager.GetSection("jobs") as Jobs;
                foreach (var j in jobsSection.jobs)
                {
                    ExecuteJob(j as JobSettings);
                }
            }
            catch (Exception ex)
            {
                if (ex.InnerException != null)
                    Console.WriteLine(ex.InnerException.Message);
                Console.WriteLine(ex.Message);
                IsRestarting = true;
            }
            finally
            {
                WriteDivider();
                Console.WriteLine("Работа завершена {0}", DateTime.Now.ToLongTimeString());
                Restart();
            }
        }
        static void ExecuteJob(JobSettings job)
        {
            WriteDivider();
            Console.WriteLine("Выполнение задания: {0} [{1}] dataKey=\"{2}\"", job.Name, job.Type.ToString(), job.DataKey);
            WriteDivider();
            var srcSC = ConfigurationManager.ConnectionStrings[job.Source];
            var dstSC = ConfigurationManager.ConnectionStrings[job.Target];

            if (srcSC == null)
            {
                Console.WriteLine("Не найдена строка подключения {0}", job.Source);
                WriteDivider();
                return;
            }
            if (dstSC == null)
            {
                Console.WriteLine("Не найдена строка подключения {0}", job.Target);
                WriteDivider();
                return;
            }
            Console.WriteLine("Источник:\t{0} [{1}]", job.Source, srcSC.ConnectionString);
            Console.WriteLine("Назначение:\t{0} [{1}]", job.Target, dstSC.ConnectionString);
            if (job.Type == JobType.Journal)
            {
                Console.WriteLine("Префикс источника:\t{0}", job.SourceProcedurePrefix);
                Console.WriteLine("Префикс приемника:\t{0}", job.TargetProcedurePrefix);
                Console.WriteLine("Задание запущено:\t{0}", DateTime.Now.ToLongTimeString());
                (new SqlTransfer(job, srcSC.ConnectionString, dstSC.ConnectionString)).Run();
                Console.WriteLine("Задание завершено:\t{0}", DateTime.Now.ToLongTimeString());
            }
            else if (job.Type == JobType.Start)
            {
                // Остатки на начало
                Console.WriteLine("Префикс источника:\t{0}", job.SourceProcedurePrefix);
                Console.WriteLine("Префикс приемника:\t{0}", job.TargetProcedurePrefix);
                Console.WriteLine("Задание запущено:\t{0}", DateTime.Now.ToLongTimeString());
                (new SqlTransfer(job, srcSC.ConnectionString, dstSC.ConnectionString)).RunStart();
                Console.WriteLine("Задание завершено:\t{0}", DateTime.Now.ToLongTimeString());
            }
            else if (job.Type == JobType.Catalog)
            {
                // Каталоги
                Console.WriteLine("Префикс источника:\t{0}", job.SourceProcedurePrefix);
                Console.WriteLine("Префикс приемника:\t{0}", job.TargetProcedurePrefix);
                Console.WriteLine("Задание запущено:\t{0}", DateTime.Now.ToLongTimeString());
                (new SqlCatalogTransfer(job, srcSC.ConnectionString, dstSC.ConnectionString)).Run();
                Console.WriteLine("Задание завершено:\t{0}", DateTime.Now.ToLongTimeString());
            }
            else if (job.Type == JobType.Document)
            {
                // Каталоги
                Console.WriteLine("Префикс источника:\t{0}", job.SourceProcedurePrefix);
                Console.WriteLine("Префикс приемника:\t{0}", job.TargetProcedurePrefix);
                Console.WriteLine("Использовать GUID:\t{0}", job.UseGuid);
                Console.WriteLine("Задание запущено:\t{0}", DateTime.Now.ToLongTimeString());
                (new SqlDocumentTransfer(job, srcSC.ConnectionString, dstSC.ConnectionString)).Run();
                Console.WriteLine("Задание завершено:\t{0}", DateTime.Now.ToLongTimeString());
            }
        }
    }
}
