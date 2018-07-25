using System;
using System.Data;
using System.Configuration;

namespace Tier.Cmd.Classes
{
    internal static class PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO
    {
        internal static void RecolectarDatos()
        {
            string[] hojasDocumento = { "sheet1", "sheet2","hoja3" };
            DataSet dataSet = new DataSet();
            string path = ConfigurationManager.AppSettings["RutaLocalMDMparametricas"];
            string archivo = ConfigurationManager.AppSettings["NombreArchivoCargue"];
            dataSet = MetodosGlobales.ReadExcelFile(hojasDocumento, path + archivo);
        }
    }
}
