using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;

namespace Tier.Cmd.Classes
{
    internal static class PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO
    {
        internal static void RecolectarDatos()
        {
            string rutaCompleta = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string rutaArchivo = string.Format("{0}\\Recursos\\DataMig.xls", rutaCompleta.Substring(0, rutaCompleta.LastIndexOf("\\")));
            Console.WriteLine(rutaArchivo);
            
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            var constructorConsulta = Builders<Dto.PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO>.Filter;
            var filtros = constructorConsulta.Empty;
           filtros = filtros & constructorConsulta.Eq(x => x.producto, "Conectividad Avanzada IP");
            
            var lista = db.GetCollection<Dto.PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO>("PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO").Find(filtros).ToList();

            string[] hojasDocumento = { "PRODUCTOS" };
            DataSet dataSet = new DataSet();

            dataSet = MetodosGlobales.ReadExcelFile(hojasDocumento, rutaArchivo);

            Console.ReadLine();
        }
    }
}
