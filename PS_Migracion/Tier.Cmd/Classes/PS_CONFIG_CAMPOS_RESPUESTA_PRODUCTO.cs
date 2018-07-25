using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
using System.Data;
using System.Configuration;
//using MongoDB.Driver;

namespace Tier.Cmd.Classes
{
    internal static class PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO
    {
        internal static void RecolectarDatos()
        {
            string[] hojasDocumento = { "sheet1", "sheet2","hoja3" };
            DataSet dataSet = new DataSet();
            //DataTable tabla = new DataTable();
            string path = ConfigurationManager.AppSettings["RutaLocalMDMparametricas"];
            string archivo = ConfigurationManager.AppSettings["NombreArchivoCargue"];
            //IMongoDatabase db = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString()).GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());
            dataSet = MetodosGlobales.ReadExcelFile(hojasDocumento, path + archivo);

        }
    }
}
