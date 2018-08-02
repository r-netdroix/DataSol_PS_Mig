using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;

namespace Tier.Cmd.Classes
{
    internal static class PS_USUARIO
    {
        internal static void RecolectarDatos()
        {
            string rutaCompleta = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string rutaArchivo = string.Format("{0}\\Recursos\\DataMig.xls", rutaCompleta.Substring(0, rutaCompleta.LastIndexOf("\\")));
            string[] hojasDocumento = { "PRODUCTOS", "USUARIOS" };


            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            FilterDefinitionBuilder<Dto.PS_USUARIO> constructorConsulta = Builders<Dto.PS_USUARIO>.Filter;
            FilterDefinition<Dto.PS_USUARIO> filtros = constructorConsulta.Empty;
            IMongoCollection<Dto.PS_USUARIO> coleccion_usuarios = db.GetCollection<Dto.PS_USUARIO>("PS_USUARIO");

            DataSet dataSet = new DataSet();
            DateTime fechahoy = DateTime.Now;
            dataSet = MetodosGlobales.ReadExcelFile(hojasDocumento, rutaArchivo);
            DataTable tabla_usuarios = dataSet.Tables["USUARIOS"];
            List<Dto.PS_USUARIO> lista_usuarios = tabla_usuarios.AsEnumerable().Select(x => new Dto.PS_USUARIO()
            {
                Id = ObjectId.GenerateNewId().ToString(),
                FechaCreacion = fechahoy,
                UsuarioCreacion = "GESTOR",
                username = x[0] != DBNull.Value ? x[0].ToString().Trim() : null,
                id_rol = "ObjectId('5b19ac3298ff551fccbb3e18')",
                rol = "GESTOR",
                id_tipo_identificacion = "CC",
                tipo_identificacion = "CÉDULA DE CIUDADANÍA",
                identificacion = x[4] != DBNull.Value ? x[4].ToString().Trim() : null,
                nombres = x[2] != DBNull.Value ? x[2].ToString().Trim().ToUpper() : null,
                es_activo= x[1].ToString() == "Activo" ? true : false,
                apellidos = ""
            }).ToList();
            foreach (Dto.PS_USUARIO usuario in lista_usuarios)
            {
                if (usuario.es_activo == true)
                {
                    if (string.IsNullOrEmpty(usuario.nombres) )
                    {
                        usuario.apellidos = "N/A";
                    }
                    else
                    {
                        string apellidos = "", nombres = "";
                        MetodosGlobales.ObtenerDatosNombre(usuario.nombres, out nombres, out apellidos);
                        usuario.nombres = nombres;
                        usuario.apellidos = apellidos;
                        Console.WriteLine("nombre: " + nombres + " apellidos: " + apellidos);
                    }
                }
                
            }
            
            lista_usuarios=lista_usuarios.Where(x => x.es_activo == true).ToList();
            Console.WriteLine("Registros a ingresar: " + lista_usuarios.Count);
            coleccion_usuarios.InsertMany(lista_usuarios);
            
            Console.ReadLine();
        }
    }
}
