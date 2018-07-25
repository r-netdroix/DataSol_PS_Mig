using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
/// <summary>
/// Summary description for Class1
/// </summary>
namespace Tier.Cmd.Classes
{
    internal static class MetodosGlobales
    {
        internal static DataSet ReadExcelFile(string[] hojasDocumento, string path)
        {
            DataSet ds = new DataSet();
            int i = 0;

            foreach (string sheet in hojasDocumento)
            {
                using (OleDbConnection conn = new OleDbConnection())
                {
                    string Import_FileName = path;
                    string fileExtension = Path.GetExtension(Import_FileName);
                    if (fileExtension == ".xls")
                        conn.ConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + Import_FileName + ";" + "Extended Properties='Excel 8.0;HDR=YES;'";
                    if (fileExtension == ".xlsx")
                        conn.ConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + Import_FileName + ";" + "Extended Properties='Excel 12.0 Xml;HDR=YES;'";
                    using (OleDbCommand comm = new OleDbCommand())
                    {
                        comm.CommandText = "Select * from [" + sheet + "$]";
                        comm.Connection = conn;

                        using (OleDbDataAdapter da = new OleDbDataAdapter())
                        {
                            da.SelectCommand = comm;
                            da.Fill(ds);
                            ds.Tables[i].TableName = sheet;
                            ds.AcceptChanges();
                            i++;
                        }
                    }
                }


            }
            return ds;


        }


    }
}


