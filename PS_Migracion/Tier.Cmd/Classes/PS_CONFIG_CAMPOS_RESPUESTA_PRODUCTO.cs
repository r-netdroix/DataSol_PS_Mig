using System;
namespace Tier.Cmd
{
    internal static class PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO
    {

        internal static void recolectarDatos()
        {
            string[] hojasDocumento= { "hoja1", "hoja2" };
            
            //string path = ConfigurationManager.AppSettings["RutaLocalMDMparametricas"];
            //string path = @"E:\Ejecutables_MDM\MDM_Cargue_Parametricas_PQR\";
            //string archivo = ConfigurationManager.AppSettings["NombreArchivoCargue"];

            foreach (string hoja in hojasDocumento)
            {
                MetodosGlobales.ReadExcelFile(hoja,"path+archivo");

            }
            
        }
    }
}

