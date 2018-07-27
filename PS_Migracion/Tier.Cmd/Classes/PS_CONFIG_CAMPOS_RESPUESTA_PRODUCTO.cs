using System;
using System.Data;
using System.Configuration;

namespace Tier.Cmd.Classes
{
    internal static class PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO
    {
        internal static void RecolectarDatos()
        {
            string rutaCompleta = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string rutaArchivo = string.Format("{0}\\Recursos\\DataMig.xls", rutaCompleta.Substring(0, rutaCompleta.LastIndexOf("\\")));
            Console.WriteLine(rutaArchivo);

            string[] hojasDocumento = { "sheet1", "sheet2", "hoja3" };
            DataSet dataSet = new DataSet();

            dataSet = MetodosGlobales.ReadExcelFile(hojasDocumento, rutaArchivo);

            Console.ReadLine();
        }
    }
}
