using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Configuration;
using MongoDB.Driver;

namespace Tier.Cmd.Classes
{
    internal static class PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO
    {

        internal static void RecolectarDatos()
        {

            string[] hojasDocumento = { "sheet1", "sheet2" };
            DataSet ds = new DataSet();
            DataTable tabla = new DataTable();
            string path = ConfigurationManager.AppSettings["RutaLocalMDMparametricas"];
            //string path = @"E:\Ejecutables_MDM\MDM_Cargue_Parametricas_PQR\";
            string archivo = ConfigurationManager.AppSettings["NombreArchivoCargue"];
            //IMongoDatabase db = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString()).GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            foreach (string hoja in hojasDocumento)
            {
                ds = MetodosGlobales.ReadExcelFile(hoja, path + archivo);

                tabla = ds.Tables[hoja];
               /* foreach (DataRow row in tabla.Rows)
                {
                    Console.Write(Convert.ToString(row["Nombre"]));
                    Console.Write(Convert.ToString(row["numero"]));
                }*/
            }

           // Console.ReadLine();
        }
    }
}
