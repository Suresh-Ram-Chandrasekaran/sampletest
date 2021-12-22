using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Postgresql_bulkCopy
{
    class Program
    {
        //public static Program datatype;
        public static string data = "";
        public static List<Tables> TablesList = new List<Tables>();
        public static string cs = "Host=localhost; Username=postgres; Password=Subha2022@; Database=AdventureWorks2016__";
        public static NpgsqlConnection con = new NpgsqlConnection(cs);
        public static SqlConnection conn = new SqlConnection("Data Source = localhost; User ID = sa; Password = Subha2022@; Initial Catalog = AdventureWorks2016");
        static void Main(string[] args)
        {
            using (conn)
            {
              
                conn.Open();
                ViewTables();
                //creation();
                conn.Close();
                Console.WriteLine("sucess");

            }
            Console.ReadLine();
        }

        public static void ViewTables()
        {
            using (con)
            {
                con.Open();
                var cd1 = new SqlCommand();
            cd1.CommandText = "SELECT DISTINCT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA .COLUMNS";
            cd1.CommandType = System.Data.CommandType.Text;
            cd1.Connection = conn;
            SqlDataReader r1 = cd1.ExecuteReader();
            while (r1.Read())
            {
                    CreateSchema(r1);
            }
               


            r1.Close();
            var cmd = new SqlCommand();
            cmd.CommandText = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS";
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.Connection = conn;
           
            SqlDataReader read = cmd.ExecuteReader();

            if (read.HasRows)

            {
                while (read.Read())
                {
                    TablesList.Add(new Tables
                    {
                        TablesName = read["TABLE_NAME"].ToString()

                    });


                }
            }
            read.Close();
            List<Tables> List = new List<Tables>(TablesList);
            var cd = new SqlCommand();


            foreach (var i in List)
            {


                cd.CommandText = "select * from information_schema.columns ";
                cd.CommandType = System.Data.CommandType.Text;
                cd.Connection = conn;
                SqlDataReader reader = cd.ExecuteReader();
                reader.Close();
            }


               // CreateSchema(r1);
                r1.Close();
                Createdatabase();


              
              createtable(List, cd1);
                con.Close();
            }
        }


       


        public static void CreateSchema(SqlDataReader SValues)
        {
            
            var command1 = new NpgsqlCommand();
            command1.Connection = con;
            command1.CommandText = "DROP TABLE IF EXISTS " + SValues["TABLE_SCHEMA"] + "." + SValues["TABLE_NAME"] + "";
            command1.ExecuteNonQuery();

            command1.CommandText="DROP SCHEMA IF EXISTS "+SValues["TABLE_SCHEMA"]+" CASCADE";
        
            command1.ExecuteNonQuery();

            command1.CommandText = "CREATE SCHEMA " + SValues["TABLE_SCHEMA"] + "";
            command1.ExecuteNonQuery();

        
        }
        public static void Createdatabase()
        {
            var cmd = new NpgsqlCommand();
            cmd.Connection = con;

            cmd.CommandText = "DROP DATABASE IF EXISTS AdventureWorks2016__";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "CREATE DATABASE AdventureWorks2016__";
            cmd.ExecuteNonQuery();
        }
        public static void createtable(List<Tables> list,SqlCommand cd1)
        {
            
            
            SqlDataReader schemareader = cd1.ExecuteReader();

            var cmd = new NpgsqlCommand();
            cmd.Connection = con;
            
            while (schemareader.Read())
            {
              
                 var datatype=schemareader["DATA_TYPE"];
               
                switch (datatype)
                {
                    case "tinyint":
                        {
                            data = "smallint";
                            break;
                        }
                    case "smallmoney":
                        {
                            data = "money";
                            break;
                        }

                    case "nvarchar":
                        {
                            data = "varchar";
                            break;
                        }

                    case "datatime":
                        {
                            data = "timestamp(3)";
                            break;
                        }

                    case "uniqueidentifier":
                        {
                            data = "char(16)";
                            break;
                        }

                    case "bit":
                        {
                            data = "boolean";
                            break;
                        }

                    case "nchar":
                        {
                            data = "char";
                            break;
                        }
                    case "varbinary":
                        {
                            data = "Bytea";
                            break;
                        }
                    case "money":
                        {
                            data = "money";
                            break;
                        }
                    case "date":
                        {
                            data = "date";
                            break;
                        }
                    case "xml":
                        {
                            data = "xml";
                            break;
                        }
                    case "decimal":
                        {
                            data = "decimal";
                            break;
                        }
                    case "smallint":
                        {
                            data = "smallint";
                            break;
                        }
                    case "int":
                        {
                            data = "int";
                            break;
                        }
                    case "integer":
                        {
                            data = "integer";
                            break;
                        }





                }


                    cmd.CommandText = "DROP TABLE IF EXISTS " + schemareader["TABLE_SCHEMA"] + "." + schemareader["TABLE_NAME"] + "";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE TABLE " + schemareader["TABLE_SCHEMA"] + "." + schemareader["TABLE_NAME"] + "()";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "ALTER TABLE " + schemareader["TABLE_SCHEMA"] + "." + schemareader["TABLE_NAME"] +" ADD COLUMN "+schemareader["COLUMN_NAME"].ToString()+" "+ data +"";
                    cmd.ExecuteNonQuery();
            }

        }

        //ALTER TABLE table_name 
        //ADD COLUMN column_name datatype column_constraint;
     
    }
}
