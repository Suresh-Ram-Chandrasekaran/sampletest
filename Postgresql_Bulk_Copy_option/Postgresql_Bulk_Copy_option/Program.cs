using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Postgresql_Bulk_Copy_option
{
    class Program
    {
        public static SqlConnection connect = new SqlConnection("Data Source=localhost;User ID=sa;Password=boldsql@123;Initial Catalog=AdventureWorks2016");
        public static NpgsqlConnection postgreconnection = new NpgsqlConnection("Host=localhost;Username=postgres;Password=boldsql@123;Database=adventureworks2016");
        public static List<Tablelists> tablelists = new List<Tablelists>();
        public static List<DistinctTableschema> val = new List<DistinctTableschema>();
        static void Main(string[] args)
        {
            using (connect)
            
            {
                postgreconnection.Open();
                connect.Open();

                Console.WriteLine("Connected");
                ReadValuesFromSql();
                MakeTable();
                connect.Close();
                postgreconnection.Close();

            }
            
            Console.ReadLine();
        }
        public static void ReadValuesFromSql()
        {

            List<DistinctTableschema> tableschema = new List<DistinctTableschema>();
            var command1 = new SqlCommand();
            command1.CommandText = "SELECT DISTINCT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS";
            command1.CommandType = System.Data.CommandType.Text;
            command1.Connection = connect;
            

            SqlDataReader read3 = command1.ExecuteReader();
            while (read3.Read())
            {
                var postgrecommand5 = new NpgsqlCommand();
                postgrecommand5.CommandText = "DROP TABLE IF EXISTS " + read3["TABLE_SCHEMA"] + "." + read3["TABLE_NAME"] + "";
                postgrecommand5.CommandType = System.Data.CommandType.Text;
                postgrecommand5.Connection = postgreconnection;
                postgrecommand5.ExecuteNonQuery();
            }
            read3.Close();
            SqlDataReader read1 = command1.ExecuteReader();
            
            while (read1.Read())
            {
                

                var postgrecommand3 = new NpgsqlCommand();
                postgrecommand3.CommandText = "DROP SCHEMA IF EXISTS " +read1["TABLE_SCHEMA"] + "";
                postgrecommand3.CommandType = System.Data.CommandType.Text;
                postgrecommand3.Connection = postgreconnection;
                postgrecommand3.ExecuteNonQuery();
               
                var postgrecommand2 = new NpgsqlCommand();
                postgrecommand2.CommandText = "CREATE SCHEMA " + read1["TABLE_SCHEMA"] + "";
                postgrecommand2.CommandType = System.Data.CommandType.Text;
                postgrecommand2.Connection = postgreconnection;
                postgrecommand2.ExecuteNonQuery();

               
            }
            read1.Close();
            
            SqlDataReader read2 = command1.ExecuteReader();
            while (read2.Read())
            {
                
                var postgrecommand4 = new NpgsqlCommand();
                postgrecommand4.CommandText = "CREATE TABLE " + read2["TABLE_SCHEMA"] + "." + read2["TABLE_NAME"] + " ()";
                postgrecommand4.CommandType = System.Data.CommandType.Text;
                postgrecommand4.Connection = postgreconnection;
                postgrecommand4.ExecuteNonQuery();
                

            }

            read2.Close();
           
            

            var command2 = new SqlCommand();
            command2.CommandText = "SELECT DISTINCT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS";
            command2.CommandType = System.Data.CommandType.Text;
            command2.Connection = connect;
            SqlDataReader read4 = command2.ExecuteReader();
           
            
            while (read4.Read())
            {
                string varchartype = read4["DATA_TYPE"].ToString().Replace("nvarchar", "varchar").Replace("tinyint","smallint").Replace("hierarchyid", "varchar").Replace("uniqueidentifier","char")
                    .Replace("geography","varchar").Replace("varbinary","varchar").Replace("smallmoney","money").Replace("datetime", "timestamp");
                string mystring = read4["COLUMN_NAME"].ToString();
                string newstring = mystring.Replace(' ','_');
                string removedots = newstring.Replace('.','_');
                //Console.WriteLine(removedots + " "+read4["TABLE_NAME"]+" "+read4["TABLE_SCHEMA"]);
                var postgrecommand5 = new NpgsqlCommand();
                
                postgrecommand5.CommandText = "alter table " + read4["TABLE_SCHEMA"] + "." + read4["TABLE_NAME"] + " add column " + "new_"+removedots.ToString()+"_" + " "+varchartype +"";
                postgrecommand5.CommandType = System.Data.CommandType.Text;
                postgrecommand5.Connection = postgreconnection;
                postgrecommand5.ExecuteNonQuery();
                
            }
            read4.Close();
        }
        public static void MakeTable()
        {
            
                
            var sqlcommand2 = new SqlCommand();
            sqlcommand2.CommandText = "SELECT DISTINCT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS";
            sqlcommand2.CommandType = System.Data.CommandType.Text;
            sqlcommand2.Connection = connect;
            SqlDataReader reader2 = sqlcommand2.ExecuteReader();

           
            while (reader2.Read())
            {
                string mystring = reader2["COLUMN_NAME"].ToString().Replace(' ', '_').Replace('.', '_');
                
                tablelists.Add(new Tablelists
                {
                    ColumnName = reader2["Column_name"].ToString(),
                    TableName = reader2["Table_Name"].ToString(),
                    TableSchema=reader2["Table_schema"].ToString(),
                    
                });
                
                
            }
            reader2.Close();
            ViewTable();
           
        }
        public static void ViewTable()
        {
            //int j = 0;
            DataTable tablename = new DataTable("TABLE_NAME");
            DataColumn dataColumn;
            DataRow row;
            foreach (var i in tablelists)
            {
                
                var sqlcomman3 = new SqlCommand();
                sqlcomman3.CommandText = "SELECT CAST([" + i.ColumnName+"]  as nvarchar(MAX)) as [convert "+i.ColumnName+"] from " + i.TableSchema + "." + i.TableName + "";
               
   
                sqlcomman3.CommandType = System.Data.CommandType.Text;
                sqlcomman3.Connection = connect;
                SqlDataReader reader5 = sqlcomman3.ExecuteReader();
            

                reader5.Read();

                if (reader5.HasRows)
                {
                    //    //Console.WriteLine(reader5.GetValue(0).ToString());
                    //tablename = new DataTable(i.TableName);
                    // DataSet dataSet = new DataSet();
                    // dataColumn = new DataColumn();

                    // dataColumn.ColumnName = i.ColumnName;
                    // tablename.Columns.Add(dataColumn);
                    // dataSet.Tables.Add(tablename);
                    // row = tablename.NewRow();
                    // row[i.ColumnName] = reader5.GetValue(0).ToString();
                    // tablename.Rows.Add(row);
                    //NpgsqlCommand cmd1 = new NpgsqlCommand();
                    //////////NpgsqlTransaction transaction = postgreconnection.BeginTransaction();
                    //////////Console.WriteLine("COPY " + reader2["TABLE_SCHEMA"] + "." + reader2["TABLE_NAME"].ToString().ToLower() + " FROM STDIN where column_name='new_databaseversion_'");


                    //////////Console.WriteLine("COPY great TO " + reader2["TABLE_SCHEMA"] + "." + reader2["TABLE_NAME"].ToString().ToLower() + " ");
                    //////Console.WriteLine("Insert into " + i.TableSchema.ToString().ToLower() + "." + i.TableName.ToString().ToLower() + "(new_" + i.ColumnName.ToLower() + "_) values ('"+reader5.GetValue(0).ToString()+"'); ");
                    //cmd1.CommandText = "Insert into " + i.TableSchema.ToString().ToLower() + "." + i.TableName.ToString().ToLower() + "(new_" + i.ColumnName.ToLower() + "_) values (@values1); ";
                    //cmd1.Parameters.AddWithValue("@values1", reader5.GetValue(0).ToString());
                    //cmd1.CommandType = System.Data.CommandType.Text;
                    //cmd1.Connection = postgreconnection;
                    //cmd1.ExecuteNonQuery();
                    using (postgreconnection)
                    {
                        Console.WriteLine(reader5.GetValue(0).ToString());

                        using (var writer = postgreconnection.BeginTextImport("COPY dbo.awbuildversion (" + reader5.GetValue(0).ToString() + ") FROM STDIN")) 
                        {

                        }

                    }

                }
                reader5.Close();
                //Console.WriteLine("COPY " + i.TableSchema + "." +i.TableName  + " FROM STDIN");

                //foreach (DataRow dataRow in tablename.Rows)
                //{
                //    foreach (var item in dataRow.ItemArray)
                //    {

                //        val.Add(new DistinctTableschema
                //        {
                //            values = item.ToString(),
                //        }) ;

                //    }


                //}


                

                   
               







            }
            

            //var sqlcommand2 = new SqlCommand();
            //sqlcommand2.CommandText = "SELECT DISTINCT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS";
            //sqlcommand2.CommandType = System.Data.CommandType.Text;
            //sqlcommand2.Connection = connect;
            //SqlDataReader reader2 = sqlcommand2.ExecuteReader();
            //while (reader2.Read())
            //{
            //    NpgsqlCommand cmd1 = new NpgsqlCommand();
            //    ////NpgsqlTransaction transaction = postgreconnection.BeginTransaction();
            //    ////Console.WriteLine("COPY " + reader2["TABLE_SCHEMA"] + "." + reader2["TABLE_NAME"].ToString().ToLower() + " FROM STDIN where column_name='new_databaseversion_'");


            //    ////Console.WriteLine("COPY great TO " + reader2["TABLE_SCHEMA"] + "." + reader2["TABLE_NAME"].ToString().ToLower() + " ");
            //    Console.WriteLine("Insert into " + reader2["TABLE_SCHEMA"].ToString().ToLower() + "." + reader2["TABLE_NAME"].ToString().ToLower() + "values (new_" + reader2["COLUMN_NAME"] + "_) values (); ");
            //    cmd1.CommandText = "Insert into " + reader2["TABLE_SCHEMA"].ToString().ToLower() + "." + reader2["TABLE_NAME"].ToString().ToLower() + "(new_" + reader2["COLUMN_NAME"] + "_) values ('200'); ";
            //    cmd1.CommandType = System.Data.CommandType.Text;
            //    cmd1.Connection = postgreconnection;
            //    cmd1.ExecuteNonQuery();
            //}

            //reader2.Close();


           


        }
       




    }
}
/*************************************************My tries***************************************************/

//public static void CreateTable()
//{
//    var postgrecommand = new NpgsqlCommand();
//    postgrecommand.CommandText = "DROP DATABASE IF EXISTS adventure";
//    postgrecommand.CommandType = System.Data.CommandType.Text;
//    postgrecommand.Connection = postgreconnection;
//    postgrecommand.ExecuteNonQuery();

//    var postgrecommand1 = new NpgsqlCommand();
//    postgrecommand1.CommandText = "CREATE DATABASE adventure";
//    postgrecommand1.CommandType = System.Data.CommandType.Text;
//    postgrecommand1.Connection = postgreconnection;
//    postgrecommand1.ExecuteNonQuery();
//}





//foreach(var k in distinct)
//{



//   
//    //var postgrecommand5 = new NpgsqlCommand();
//    //postgrecommand5.CommandText = "DROP TABLE IF EXISTS " + k.TableSchema + "." + k.Tablelist + "";
//    //postgrecommand5.CommandType = System.Data.CommandType.Text;
//    //postgrecommand5.Connection = postgreconnection;
//    //postgrecommand5.ExecuteNonQuery();

//    //var postgrecommand3 = new NpgsqlCommand();
//    //postgrecommand3.CommandText = "CREATE TABLE "+k.TableSchema+"."+ k.Tablelist + "(Id int)";
//    //postgrecommand3.CommandType = System.Data.CommandType.Text;
//    //postgrecommand3.Connection = postgreconnection;
//    //postgrecommand3.ExecuteNonQuery();

//}






//private static DataTable MakeTable(SqlDataReader read1)
//{
//}
//public static void CreatepostgreConnection(SqlDataReader read1)
//{
//    using (postgreconnection)
//    {
//        postgreconnection.Open();
//        SqlBulkCopy copy = new SqlBulkCopy(connection)
//        {

//        }
//        postgreconnection.Close();
//    }
//}

//while (read.Read())
//{

//    foreach (DataRow dataRow in tablename.Rows)
//    {
//        foreach (var item in dataRow.ItemArray)
//        {
//            NpgsqlCommand cmd1 = new NpgsqlCommand();
//            Console.WriteLine("COPY " + item + " TO " + i + "." + read["TABLE_NAME"] + " ");
//            cmd1.CommandText = "COPY " + item + " TO " + i + "." + read["TABLE_NAME"] + " ";
//            cmd1.CommandType = System.Data.CommandType.Text;
//            cmd1.Connection = postgreconnection;
//            cmd1.ExecuteNonQuery();


//        }


//    }

//}

//foreach (var i in values)
//{
//    NpgsqlCommand cmd = new NpgsqlCommand();
//    cmd.CommandText = "SELECT * FROM information_schema.tables WHERE table_schema = '"+i.ToLower()+"'; ";
//    cmd.CommandType = System.Data.CommandType.Text;
//    cmd.Connection = postgreconnection;
//    NpgsqlDataReader read = cmd.ExecuteReader();

//    read.Close();
//}
//using (var writer = postgreconnection.BeginTextImport("COPY "+i.TableSchema+"."+i.TableName+" FROM STDIN"))
//{
//        foreach (DataRow dataRow in tablename.Rows)
//        {
//            foreach (var item in dataRow.ItemArray)
//            {
//                writer.Write(item);

//            }


//        }


//}


//newmethod(tablename);
