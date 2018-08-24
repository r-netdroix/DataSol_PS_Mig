using System;
using System.Configuration;
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
            if (!string.IsNullOrEmpty(NombreCliente))
            {
                string[] sPalabrasNombre = NombreCliente.Split(' ');

                if (sPalabrasNombre.Length == 4)
                {
                    Nombres = sPalabrasNombre[0] + " " + sPalabrasNombre[1];
                    Apellidos = sPalabrasNombre[2] + " " + sPalabrasNombre[3];
                }
                else if (sPalabrasNombre.Length == 1)
                {
                    Nombres = sPalabrasNombre[0];
                    Apellidos = "Sin Apellido";
                }
                else if (sPalabrasNombre.Length == 2)
                {
                    Nombres = sPalabrasNombre[0];
                    Apellidos = sPalabrasNombre[1];
                }
                else if (sPalabrasNombre.Length == 3)
                {
                    Nombres = sPalabrasNombre[0];
                    Apellidos = sPalabrasNombre[1] + " " + sPalabrasNombre[2];
                }
                else if (sPalabrasNombre.Length > 4)
                {
                    Nombres = sPalabrasNombre[0] + " " + sPalabrasNombre[1];
                    Apellidos = string.Empty;
                    for (int i = 2; i < sPalabrasNombre.Length; i++)
                    {
                        Apellidos += sPalabrasNombre[i] + " ";
                    }
                }
                else
                {
                    Nombres = null;
                    Apellidos = null;
                }
            }
            else
            {
                Nombres = null;
                Apellidos = null;
            }

            return true;
        }
    }

    public class prcManejoErrores
    {
        public void ErroresGeneral(Exception Excepcion, string sNombreArchivo, string sMensajeAdicional = "")
        {
            try
            {
                bool IsExists = System.IO.Directory.Exists(ConfigurationManager.AppSettings["RutaLog"]);
                if (!IsExists)
                    System.IO.Directory.CreateDirectory(ConfigurationManager.AppSettings["RutaLog"]);
            }
            catch { }

            //Registrar error en Log
            StreamWriter sw = null;
            string NombreArchivoLog = ConfigurationManager.AppSettings["RutaLog"].ToString() + sNombreArchivo + ".txt";
            try
            {
                sw = new StreamWriter(NombreArchivoLog, true);
                string sMensajeLog = "-------------------------------------";
                sw.WriteLine(sMensajeLog);
                sMensajeLog = "Fecha y Hora: " + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Day.ToString())) + "-" + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Month.ToString())) + "-" + string.Format("{0:0000}", Convert.ToInt32(System.DateTime.Now.Year.ToString())) + " " + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Hour.ToString())) + ":" + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Minute.ToString())) + ":" + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Second.ToString()));
                sw.WriteLine(sMensajeLog);
                if (Excepcion != null)
                {
                    sMensajeLog = "Fuente: " + Excepcion.Source;
                    sw.WriteLine(sMensajeLog);
                    sMensajeLog = "Mensaje: " + Excepcion.Message;
                    sw.WriteLine(sMensajeLog);
                }
                if (sMensajeAdicional.Length > 0)
                {
                    sMensajeLog = "Observacion Adicional: " + sMensajeAdicional;
                    sw.WriteLine(sMensajeLog);
                }
            }
            catch { }
            finally
            {
                try { sw.Close(); }
                catch { }
            }
        }

        public void GuardarRegistroSQL(string sNombreArchivo, string sConsulta)
        {
            try
            {
                bool IsExists = System.IO.Directory.Exists(ConfigurationManager.AppSettings["RutaLog"]);
                if (!IsExists)
                    System.IO.Directory.CreateDirectory(ConfigurationManager.AppSettings["RutaLog"]);
            }
            catch { }

            //Registrar error en Log
            StreamWriter sw = null;
            string NombreArchivoLog = ConfigurationManager.AppSettings["RutaLog"].ToString() + sNombreArchivo + ".txt";
            try
            {
                sw = new StreamWriter(NombreArchivoLog, true);
                string sMensajeLog = sConsulta;
                sw.WriteLine(sMensajeLog);
            }
            catch { }
            finally
            {
                try { sw.Close(); }
                catch { }
            }
        }

        public void LogGeneral(String message, String item, string sNombreArchivo, string sMensajeAdicional = "")
        {
            try
            {
                bool IsExists = System.IO.Directory.Exists(ConfigurationManager.AppSettings["RutaLog"]);
                if (!IsExists)
                    System.IO.Directory.CreateDirectory(ConfigurationManager.AppSettings["RutaLog"]);
            }
            catch { }

            //Registrar error en Log
            StreamWriter sw = null;
            string NombreArchivoLog = ConfigurationManager.AppSettings["RutaLog"].ToString() + sNombreArchivo + ".txt";
            try
            {
                sw = new StreamWriter(NombreArchivoLog, true);
                string sMensajeLog = "-------------------------------------";
                sw.WriteLine(sMensajeLog);
                sMensajeLog = "Fecha y Hora: " + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Day.ToString())) + "-" + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Month.ToString())) + "-" + string.Format("{0:0000}", Convert.ToInt32(System.DateTime.Now.Year.ToString())) + " " + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Hour.ToString())) + ":" + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Minute.ToString())) + ":" + string.Format("{0:00}", Convert.ToInt32(System.DateTime.Now.Second.ToString()));
                sw.WriteLine(sMensajeLog);
                if (message != null)
                {
                    sMensajeLog = "Fuente: " + message;
                    sw.WriteLine(sMensajeLog);
                    sMensajeLog = "Mensaje: " + item;
                    sw.WriteLine(sMensajeLog);
                }
                if (sMensajeAdicional.Length > 0)
                {
                    sMensajeLog = "Observacion Adicional: " + sMensajeAdicional;
                    sw.WriteLine(sMensajeLog);
                }
            }
            catch { }
            finally
            {
                try { sw.Close(); }
                catch { }
            }
        }
    }

    //class PublicarArchivo
    //{
    //    public static bool PublicarArchivoIVR(string sNombreArchivoPublicar)
    //    {
    //        try
    //        {
    //            bool IsExists = System.IO.Directory.Exists(ConfigurationManager.AppSettings["RutaArchivosGenerados"]);
    //            if (!IsExists)
    //                System.IO.Directory.CreateDirectory(ConfigurationManager.AppSettings["RutaArchivosGenerados"]);
    //        }
    //        catch { }

    //        bool bRetorno = false;

    //        //Obtener Archivos via sFTP a Ruta Local
    //        SessionOptions sessionOptionsOCS = new SessionOptions
    //        {
    //            Protocol = Protocol.Ftp,
    //            HostName = ConfigurationManager.AppSettings["FTPIVR"].ToString(),
    //            UserName = ConfigurationManager.AppSettings["UserFTPIVR"].ToString(),
    //            Password = ConfigurationManager.AppSettings["PassFTPIVR"].ToString()
    //        };

    //        using (Session sessionOCS = new Session())
    //        {
    //            try
    //            {
    //                sessionOCS.Open(sessionOptionsOCS);
    //                sessionOCS.Timeout = new TimeSpan(0, 0, 45);
    //                TransferOperationResult transferResult;
    //                TransferOptions transferOptions;

    //                transferOptions = new TransferOptions();
    //                transferOptions.TransferMode = TransferMode.Binary;

    //                //Publicar
    //                transferResult = sessionOCS.PutFiles(ConfigurationManager.AppSettings["RutaArchivosGenerados"] + sNombreArchivoPublicar, ConfigurationManager.AppSettings["RutaFTPIVR"].ToString() + sNombreArchivoPublicar, false, transferOptions);
    //                transferResult.Check();

    //                bRetorno = true;
    //            }
    //            catch (Exception ex)
    //            {
    //                //Enviar a Archivo de Log Errores
    //                string sNombreArchivoError = "ErrorBatch_GeneraIVR";
    //                prcManejoErrores objError = new prcManejoErrores();
    //                objError.ErroresGeneral(ex, sNombreArchivoError, "Publicar Archivo a FTP IVR. Archivo " + sNombreArchivoPublicar);
    //            }
    //        }

    //        return bRetorno;
    //    }

        
    //    //public static bool PublicarArchivoExtractores(string sNombreArchivoPublicar)
    //    //{
    //    //    try
    //    //    {
    //    //        bool IsExists = System.IO.Directory.Exists(ConfigurationManager.AppSettings["RutaArchivosExtractores"]);
    //    //        if (!IsExists)
    //    //            System.IO.Directory.CreateDirectory(ConfigurationManager.AppSettings["RutaArchivosExtractores"]);
    //    //    }
    //    //    catch { }

    //    //    bool bRetorno = false;

    //    //    //Obtener Archivos via sFTP a Ruta Local
    //    //    SessionOptions sessionOptionsOCS = new SessionOptions
    //    //    {
    //    //        Protocol = Protocol.Ftp,
    //    //        HostName = ConfigurationManager.AppSettings["FTP_DWH"].ToString(),
    //    //        UserName = ConfigurationManager.AppSettings["UserFTP_DWH"].ToString(),
    //    //        Password = ConfigurationManager.AppSettings["PassFTP_DWH"].ToString()
    //    //    };

    //    //    using (Session sessionOCS = new Session())
    //    //    {
    //    //        try
    //    //        {
    //    //            sessionOCS.Open(sessionOptionsOCS);
    //    //            sessionOCS.Timeout = new TimeSpan(0, 0, 45);
    //    //            TransferOperationResult transferResult;
    //    //            TransferOptions transferOptions;

    //    //            transferOptions = new TransferOptions();
    //    //            transferOptions.TransferMode = TransferMode.Binary;

    //    //            //Publicar
    //    //            transferResult = sessionOCS.PutFiles(ConfigurationManager.AppSettings["RutaArchivosExtractores"] + sNombreArchivoPublicar, ConfigurationManager.AppSettings["RutaFTP_Dataservices_Extractores"].ToString() + sNombreArchivoPublicar, false, transferOptions);
    //    //            transferResult.Check();

    //    //            bRetorno = true;
    //    //        }
    //    //        catch (Exception ex)
    //    //        {
    //    //            //Enviar a Archivo de Log Errores
    //    //            string sNombreArchivoError = "ErrorBatch_GeneraIVR";
    //    //            prcManejoErrores objError = new prcManejoErrores();
    //    //            objError.ErroresGeneral(ex, sNombreArchivoError, "Publicar Archivo a FTP Extractores. Archivo " + sNombreArchivoPublicar);
    //    //        }
    //    //    }

    //    //    return bRetorno;
    //    //}

    //}
}


