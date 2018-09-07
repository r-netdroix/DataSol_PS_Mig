using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Cmd.Classes
{
    class CorreccionUsuarios
    {
        internal static void CorreccionGrupoUsuarios()
        {
            string path = ConfigurationManager.AppSettings["RutaArchivosExtractores"];
            try
            {
                bool IsExists = System.IO.Directory.Exists(path);
                if (!IsExists)
                    System.IO.Directory.CreateDirectory(path);
            }
            catch (Exception ex)
            {
                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                prcManejoErrores objError = new prcManejoErrores();
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en la creacion y existencia de la carpeta contenedora del archivo generado");
            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_Grupo_Usu = null;

            int Conteo_Usu = 0;
            int conteo_registros_grupos = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_Grupo_Usuarios_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_Grupo_Usu = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Coleccion_PS_Usuarios = db.GetCollection<BsonDocument>("PS_USUARIO");
                IMongoCollection<BsonDocument> Coleccion_PS_Grupo = db.GetCollection<BsonDocument>("PS_GRUPO_ASIGNACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_Usuarios = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_Usuarios = builderPS_Usuarios.Empty;
                filterPS_Usuarios = builderPS_Usuarios.SizeGte("grupos_lider", 1);
               
                List<BsonDocument> consulta_PS_Usuarios = Coleccion_PS_Usuarios.Find(filterPS_Usuarios).ToList();

                FilterDefinitionBuilder<BsonDocument> builderPS_Grupo_Asignacion = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filter_Grupo_Asignacion = builderPS_Grupo_Asignacion.Empty;

                if (consulta_PS_Usuarios != null && consulta_PS_Usuarios.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE USUARIOS A ACTUALIZAR 
                        Console.WriteLine("Registros en la coleccion de PS_ALERTAS_NOTIFICACIONES encontrados " + consulta_PS_Usuarios.Count.ToString());
                        foreach (BsonDocument itemPS_Usuarios in consulta_PS_Usuarios)
                        {
                            id_mongo = itemPS_Usuarios.GetValue("_id").ToString();
                            sTextoDescarga = "";

                            // SE BORRAN LOS REGISTROS ENCONTRADOS DENTRO DEL ARRAY DE GRUPOS LIDER

                            // SE CONSTRUYE LA CONSULTA DE LOS GRUPOS DE ASIGNACION A LOS CUALES PERTENECE
                            filter_Grupo_Asignacion = builderPS_Grupo_Asignacion.And(builderPS_Grupo_Asignacion.Eq(
                                "integrantes.id_usuario", MongoDB.Bson.ObjectId.Parse(itemPS_Usuarios.GetValue("_id").ToString())),
                                builderPS_Grupo_Asignacion.Eq("integrantes.es_lider", true));

                            //filter_Grupo_Asignacion = builderPS_Grupo_Asignacion.Eq("integrantes.id_usuario", ObjectId.Parse(id_mongo)); // NO FUNCIONA

                            // SE EJECUTA LA CONSULTA PARA ITERAR LOS IDENTIDICADORES DE LOS GRUPOS DE ASIGNACION
                            List<BsonDocument> consulta_PS_Grupo_Asignacion = Coleccion_PS_Grupo.Find(filter_Grupo_Asignacion).ToList();

                            if (consulta_PS_Grupo_Asignacion != null && consulta_PS_Grupo_Asignacion.Count() > 0)
                            {
                                List<string> lista = new List<string>();
                                foreach (var itemGrupoAsignacion in consulta_PS_Grupo_Asignacion)
                                {
                                    lista.Add(itemGrupoAsignacion.GetValue("_id").ToString());
                                    conteo_registros_grupos++;
                                }
                                sTextoDescarga = string.Format("el usuario: {0}, fue actualizado con {1} registros en grupos_lider", id_mongo, consulta_PS_Grupo_Asignacion.Count());
                                Console.WriteLine(sTextoDescarga);
                                Archivo_Grupo_Usu.WriteLine(sTextoDescarga);
                                if (pruebas == false)
                                {
                                    Coleccion_PS_Usuarios.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_Usuarios.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("grupos_lider", lista));
                                }
                            }

                            Conteo_Usu++;
                            
                        }

                        if (Conteo_Usu > 0)
                        {
                            Archivo_Grupo_Usu.Close();
                            if (pruebas == false)
                            {
                                sTextoDescarga = string.Format("Se actualizaron {0}, usuarios con {1} registros de grupos de asignacion", Conteo_Usu, conteo_registros_grupos);
                                Console.WriteLine(sTextoDescarga);
                                Archivo_Grupo_Usu.WriteLine(sTextoDescarga);
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_Ususario ultimo id actualizado" + id_mongo);
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Console.WriteLine(ex.StackTrace.ToString());
                //Enviar a Archivo de Log Errores
                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                prcManejoErrores objError = new prcManejoErrores();
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString());
                //throw ex;
            }
            finally
            {
                Archivo_Grupo_Usu.Close();
            }

        }
    }
}
