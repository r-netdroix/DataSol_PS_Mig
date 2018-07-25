using System;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;

namespace Tier.Cmd.Classes
{
    internal static class MetodosGlobales
    {
        internal static DataSet ReadExcelFile(string[] hojasDocumento, string path)
        {
            DataSet dataSet = new DataSet();
            int i = 0;

            foreach (string hoja in hojasDocumento)
            {
                using (OleDbConnection conexionOleBd = new OleDbConnection())
                {
                    string rutaDocumentoExcel = path;
                    string extensionDocumentoExcel = Path.GetExtension(rutaDocumentoExcel);
                    if (extensionDocumentoExcel == ".xls")
                        conexionOleBd.ConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + rutaDocumentoExcel + ";" + "Extended Properties='Excel 8.0;HDR=YES;'";
                    if (extensionDocumentoExcel == ".xlsx")
                        conexionOleBd.ConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + rutaDocumentoExcel + ";" + "Extended Properties='Excel 12.0 Xml;HDR=YES;'";
                    using (OleDbCommand comandoOleBd = new OleDbCommand())
                    {
                        comandoOleBd.CommandText = "Select * from [" + hoja + "$]";
                        comandoOleBd.Connection = conexionOleBd;

                        using (OleDbDataAdapter da = new OleDbDataAdapter())
                        {
                            da.SelectCommand = comandoOleBd;
                            da.Fill(dataSet);
                            dataSet.Tables[i].TableName = hoja;
                           dataSet.AcceptChanges();
                            i++;
                        }
                    }
                }


            }
            return dataSet;


        }


    }
}


