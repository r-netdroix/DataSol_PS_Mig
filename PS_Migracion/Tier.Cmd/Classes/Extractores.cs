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
    class Extractores
    {
        internal static void Extractor_PS_ALERTA(string tipo = "")
        {
            string path = ConfigurationManager.AppSettings["RutaArchivosExtractores"];
            try
            {
                bool IsExists = System.IO.Directory.Exists(path);
                if (!IsExists)
                    System.IO.Directory.CreateDirectory(path);
            }
            catch { }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_ALERTAS_NOTIFICACIONES = null;

            int Conteo_PROM = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_ALERTAS_NOTIFICACIONES_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_ALERTAS_NOTIFICACIONES = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Vol_PS = db.GetCollection<BsonDocument>("PS_ALERTAS_NOTIFICACIONES");
                FilterDefinitionBuilder<BsonDocument> builderPS_Alertas = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_Alertas = builderPS_Alertas.Empty;

                if (tipo == "")
                {
                    filterPS_Alertas = builderPS_Alertas.Or(builderPS_Alertas.Eq("Actualizacion_Extractor", "1"), !builderPS_Alertas.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    //filterPS_Alertas = builderPS_Alertas.And(
                    //       builderPS_Alertas.Eq("Fecha_extraccion", tipo),
                    //       builderPS_Alertas.Or(
                    //           builderPS_Alertas.Eq("Actualizacion_Extractor", "1"),
                    //           !builderPS_Alertas.Exists("Actualizacion_Extractor")
                    //           )
                    //       );
                    filterPS_Alertas = builderPS_Alertas.And(builderPS_Alertas.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_Alertas.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_Alertas = Vol_PS.Find(filterPS_Alertas).ToList();

                if (consulta_PS_Alertas != null && consulta_PS_Alertas.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_ALERTAS_NOTIFICACIONES encontrados " + consulta_PS_Alertas.Count.ToString());
                        foreach (BsonDocument itemPS_Alerta in consulta_PS_Alertas)
                        {
                            id_mongo = itemPS_Alerta.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_Alerta.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_Alerta.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_Alerta.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_Alerta.Contains("_id") ? !string.IsNullOrEmpty(itemPS_Alerta.GetValue("_id")?.ToString()) ? (itemPS_Alerta.GetValue("_id").ToString().Length > 30 ? itemPS_Alerta.GetValue("_id").ToString().Substring(0, 29) : itemPS_Alerta.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("fecha_creacion") && !itemPS_Alerta.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("fecha_creacion").ToString()) ? (itemPS_Alerta.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_Alerta.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_Alerta.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("usuario_creacion") && !itemPS_Alerta.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("usuario_creacion").ToString()) ? (itemPS_Alerta.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_Alerta.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_Alerta.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("fecha_actualizacion") && !itemPS_Alerta.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("fecha_actualizacion").ToString()) ? (itemPS_Alerta.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_Alerta.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_Alerta.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("fecha_modificacion") && !itemPS_Alerta.GetValue("fecha_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("fecha_modificacion").ToString()) ? (itemPS_Alerta.GetValue("fecha_modificacion").ToString().Length > 30 ? itemPS_Alerta.GetValue("fecha_modificacion").ToString().Substring(0, 30) : itemPS_Alerta.GetValue("fecha_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("id_usuario") ? !string.IsNullOrEmpty(itemPS_Alerta.GetValue("id_usuario")?.ToString()) ? (itemPS_Alerta.GetValue("id_usuario").ToString().Length > 30 ? itemPS_Alerta.GetValue("id_usuario").ToString().Substring(0, 29) : itemPS_Alerta.GetValue("id_usuario").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("descripcion") && !itemPS_Alerta.GetValue("descripcion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_Alerta.GetValue("descripcion").ToString()) ? (itemPS_Alerta.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_Alerta.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_Alerta.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("texto_alerta") && !itemPS_Alerta.GetValue("texto_alerta").IsBsonNull ? !string.IsNullOrEmpty(itemPS_Alerta.GetValue("texto_alerta").ToString()) ? (itemPS_Alerta.GetValue("texto_alerta").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_Alerta.GetValue("texto_alerta").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_Alerta.GetValue("texto_alerta").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("visto") && !itemPS_Alerta.GetValue("visto").IsBsonNull ? itemPS_Alerta.GetValue("visto").ToString().Length > 8 ? itemPS_Alerta.GetValue("visto").ToString().Substring(0, 8) : itemPS_Alerta.GetValue("visto").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("fecha_visto") && !itemPS_Alerta.GetValue("fecha_visto").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("fecha_visto").ToString()) ? (itemPS_Alerta.GetValue("fecha_visto").ToString().Length > 30 ? itemPS_Alerta.GetValue("fecha_visto").ToString().Substring(0, 30) : itemPS_Alerta.GetValue("fecha_visto").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("idSolicitud") && !itemPS_Alerta.GetValue("idSolicitud").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("idSolicitud").ToString()) ? (itemPS_Alerta.GetValue("idSolicitud").ToString().Length > 10 ? itemPS_Alerta.GetValue("idSolicitud").ToString().Substring(0, 10) : itemPS_Alerta.GetValue("idSolicitud").ToString()) : "") + //VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("tipo_solicitud") && !itemPS_Alerta.GetValue("tipo_solicitud").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("tipo_solicitud").ToString()) ? (itemPS_Alerta.GetValue("tipo_solicitud").ToString().Length > 10 ? itemPS_Alerta.GetValue("tipo_solicitud").ToString().Substring(0, 10) : itemPS_Alerta.GetValue("tipo_solicitud").ToString()) : "") + //VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_Alerta.Contains("idTareaSolicitud") && !itemPS_Alerta.GetValue("idTareaSolicitud").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("idTareaSolicitud").ToString()) ? (itemPS_Alerta.GetValue("idTareaSolicitud").ToString().Length > 30 ? itemPS_Alerta.GetValue("idTareaSolicitud").ToString().Substring(0, 29) : itemPS_Alerta.GetValue("idTareaSolicitud").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                           
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_ALERTAS_NOTIFICACIONES Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_ALERTAS_NOTIFICACIONES.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Vol_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_Alerta.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PROM++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_Alerta.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Vol_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_Alerta.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PROM++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_ALERTAS_NOTIFICACIONES ACTUALIZADA: " + itemPS_Alerta.GetValue("_id").ToString() + "Numero de PS_ALERTAS_NOTIFICACIONES actializadas: " + Conteo_PROM);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_ALERTAS_NOTIFICACIONES en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_ALERTAS_NOTIFICACIONES para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PROM > 0)
                        {
                            Archivo_PS_ALERTAS_NOTIFICACIONES.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PROM_PROMOCION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_ALERTAS_NOTIFICACIONES entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_ALERTAS_NOTIFICACIONES.Close();
            }

        }
    }
}
