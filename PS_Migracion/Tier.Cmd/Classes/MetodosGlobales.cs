using System.Data;
using System.Data.OleDb;
using System.IO;

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
        internal static bool ObtenerDatosNombre(string NombreCliente, out string Apellidos, out string Nombres)
        {
            //Determinar Nombres y Apellidos por separado
            string sNombreClienteSeparado = "";
            string sApellidoClienteSeparado = "";
            string[] sPalabrasNombre = NombreCliente.Split(' ');
            if (sPalabrasNombre.Length == 4)
            {
                sNombreClienteSeparado = sPalabrasNombre[0] + " " + sPalabrasNombre[1];
                sApellidoClienteSeparado = sPalabrasNombre[2] + " " + sPalabrasNombre[3];
            }
            else if (sPalabrasNombre.Length == 1)
            {
                sNombreClienteSeparado = sPalabrasNombre[0];
                sApellidoClienteSeparado = "Sin Apellido";
            }
            else if (sPalabrasNombre.Length == 2)
            {
                sNombreClienteSeparado = sPalabrasNombre[0];
                sApellidoClienteSeparado = sPalabrasNombre[1];
            }
            else if (sPalabrasNombre.Length == 3)
            {
                sNombreClienteSeparado = sPalabrasNombre[0];
                sApellidoClienteSeparado = sPalabrasNombre[1] + " " + sPalabrasNombre[2];
            }
            else if (sPalabrasNombre.Length > 4)
            {
                sNombreClienteSeparado = sPalabrasNombre[0] + " " + sPalabrasNombre[1];
                for (int i = 2; i < sPalabrasNombre.Length; i++)
                {
                    sApellidoClienteSeparado += sPalabrasNombre[i] + " ";
                }
            }

            Apellidos = sApellidoClienteSeparado;
            Nombres = sNombreClienteSeparado;

            return true;
        }
    }
}


