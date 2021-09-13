using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.IO.Compression;
using System.Text;
using DbfDataReader;
using Npgsql;


namespace PostrgeTest
{

    class Program
    {
        
        static void Main(string[] args)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            const string filePath = @"G:\DataBase\rawDatabase.7z";                      //название скачиваемого файла
            const string remoteUrl = "https://gnivc.ru/html/gnivcsoft/KLADR/Base.7z";   //ссылка на скачивание
            const string extractPath = @"G:\DataBase";                                  //путь распаковки
            const string dataBasePath = @"G:\DataBase\";                                //путь к файлам .DBF

            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            Download(filePath, remoteUrl, extractPath);
            dbfReader(dataBasePath,extractPath);
        }



        public static void Download(string filePath, string remoteUrl, string extractPath)
        {
            using (var myWebClient = new WebClient())
            {
                if (!File.Exists(filePath))
                {
                    //скачивание кладра
                    myWebClient.DownloadFile(remoteUrl, filePath);
                    //распаковка архива
                    ZipFile.ExtractToDirectory(filePath, extractPath); 
                }
                else
                    Console.WriteLine("Already exist");
            }
        }

        public static void dbfReader(string dataBasePath,string extractPath)
        {
            //строка подключения
            const string connString = "Host = 192.168.99.100; Username = postgres; Password = newPwd2; Database = Test";
            using var conn = new NpgsqlConnection(connString);
            conn.Open();

            //настройки запуска ридира
            var options = new DbfDataReaderOptions()
            {
                Encoding = Encoding.GetEncoding(866)
            }; 
            
            var docs = Directory.EnumerateFiles(extractPath, "*.DBF");
            foreach (var filename in docs)
            {
                var item = Path.GetFileName(filename);
                var fileName = Path.GetFileNameWithoutExtension(filename);
                Console.WriteLine(item);
                var countRows = 0;
                var counter = 0;

                //Инициализация списка с названиями колонок
                var columns = new List<string>();

                //Кол-во строк
                using (var dbfDataReader = new DbfDataReader.DbfDataReader(dataBasePath + item))
                {
                    while (dbfDataReader.Read()) countRows++;
                    dbfDataReader.Close();
                } 

                using var dbfDataReader1 = new DbfDataReader.DbfDataReader(dataBasePath + item, options);
                var count = dbfDataReader1.FieldCount;
                var temp = new string[countRows, count];
               
                //Названия колонок
                for (var j = 0; j < count; j++) columns.Add(dbfDataReader1.GetName(j));

                //sql запрос на создание таблицы и создание таблицы
                var testSql = "drop table if exists " + fileName + " ;create table " + fileName + "  (";
                for (var k = 0; k < count; k++)
                {
                    testSql += columns[k] + " VARCHAR(255)";
                    if (k != count - 1) testSql += ",";
                    if (k == count - 1) testSql += ");";
                } 
                using var cmd = new NpgsqlCommand(testSql, conn);
                cmd.ExecuteNonQuery();

                var s = DateTime.Now;
                Console.WriteLine("Начальное время перед записью в массив "+s);

                //Переписывание всех данных с исходной базы данных в думерный массив
                while (dbfDataReader1.Read())
                {
                    for (var i = 0; i < count; i++)
                    {
                        var foo = dbfDataReader1.GetString(i);
                        temp[counter, i] = foo.Replace("'","''");
                    }

                    counter++;
                } 
                
                s =  DateTime.Now;
                Console.WriteLine("После записи в массив "+s);

                //sql запрос с всеми значениями из двумерного массива старый способ
                /*
                for (var i = 0; i <countRows; i++)
                {
                    var sqlQuery1 = $"insert into {fileName} values ";
                   for (var k = 0; k < count; k++)
                   {
                       if (k == 0) sqlQuery1 += "('";
                       if (k != 0) sqlQuery1 += " ',' ";
                       sqlQuery1 += temp[i, k];
                       if (k == count-1) sqlQuery1 += "');";
                   }
                   var cmd1 =new NpgsqlCommand(sqlQuery1, conn);
                   
                   //if(i%100==0)Console.WriteLine($"Вставляю строку {i} из {countRows}");
                   cmd1.ExecuteNonQuery();
                }
                */
                //Способ инсертов быстрее чем раньше
                var sqlQuery1="";
                for (var i = 0; i <countRows; i++)
                {
                    if(i%100==0) sqlQuery1 += $"insert into {fileName} values ";
                    for (var k = 0; k < count; k++)
                    {
                        if (k == 0) sqlQuery1 += "('";
                        if (k != 0) sqlQuery1 += " ',' ";
                        sqlQuery1 += temp[i, k];
                        if (k == count-1) sqlQuery1 += "')";
                    }

                    if (i % 100 != 100-1 & i!=countRows-1) sqlQuery1 += " ,";
                    if ((i % 100 == 100-1 & i!=0) | i==countRows-1)
                    {                        
                        var cmd1 = new NpgsqlCommand(sqlQuery1, conn);
                        cmd1.ExecuteNonQuery();
                        sqlQuery1 = "";
                    }
                    
                    
                    if(i%1000==0)Console.WriteLine($"Вставляю строку {i} из {countRows}");

                }
                
                s = DateTime.Now;
                Console.WriteLine("Конечное время " +s);
            }
        }
    }
}
