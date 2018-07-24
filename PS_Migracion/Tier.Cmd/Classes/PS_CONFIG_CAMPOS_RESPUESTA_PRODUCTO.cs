using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Cmd.Classes
{
    internal static class PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO
    {

        internal static void recolectarDatos()
        {
            string[] hojasDocumento = { "hoja1", "hoja2" };

            //string path = ConfigurationManager.AppSettings["RutaLocalMDMparametricas"];
            //string path = @"E:\Ejecutables_MDM\MDM_Cargue_Parametricas_PQR\";
            //string archivo = ConfigurationManager.AppSettings["NombreArchivoCargue"];

            foreach (string hoja in hojasDocumento)
            {
                MetodosGlobales.ReadExcelFile(hoja, "path+archivo");

            }

        }
    }

}
