using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Postgresql_Bulk_Copy_option
{
    public class Tablelists
    {
        public string ColumnName { get; set; }
        public string TableName { get; set; }
        public string TableSchema { get; set; }
    }
    public class DistinctTableschema
    {
        public string values { get; set; }
    }
   
}
