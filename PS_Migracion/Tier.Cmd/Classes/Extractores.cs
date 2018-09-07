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
            catch (Exception ex)
            {
                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                prcManejoErrores objError = new prcManejoErrores();
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_ALERTAS_NOTIFICACIONES");
                
            }

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
                                        "~|" + (itemPS_Alerta.Contains("usuario_modificacion") && !itemPS_Alerta.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_Alerta.GetValue("usuario_modificacion").ToString()) ? (itemPS_Alerta.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_Alerta.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_Alerta.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
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
                                //PublicarArchivo.PublicarArchivoExtractores("PS_ALERTAS_NOTIFICACIONES_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
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

        internal static void Extractor_PS_APROBACION(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_APROBACION");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROBACION = null;

            int Conteo_PS_APROBACION = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_APROBACION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROBACION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROBACION = db.GetCollection<BsonDocument>("PS_APROBACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROBACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROBACION = builderPS_APROBACION.Empty;

                if (tipo == "")
                {
                    filterPS_APROBACION = builderPS_APROBACION.Or(builderPS_APROBACION.Eq("Actualizacion_Extractor", "1"), !builderPS_APROBACION.Exists("Actualizacion_Extractor"));
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
                    filterPS_APROBACION = builderPS_APROBACION.And(builderPS_APROBACION.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_APROBACION.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_APROBACION = Col_PS_APROBACION.Find(filterPS_APROBACION).ToList();

                if (consulta_PS_APROBACION != null && consulta_PS_APROBACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROBACION encontrados " + consulta_PS_APROBACION.Count.ToString());
                        foreach (BsonDocument itemPS_APROBACION in consulta_PS_APROBACION)
                        {
                            id_mongo = itemPS_APROBACION.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_APROBACION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_APROBACION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_APROBACION.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_APROBACION.Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("_id")?.ToString()) ? (itemPS_APROBACION.GetValue("_id").ToString().Length > 30 ? itemPS_APROBACION.GetValue("_id").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("fecha_creacion") && !itemPS_APROBACION.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("fecha_creacion").ToString()) ? (itemPS_APROBACION.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_APROBACION.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_APROBACION.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("usuario_creacion") && !itemPS_APROBACION.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("usuario_creacion").ToString()) ? (itemPS_APROBACION.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_APROBACION.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_APROBACION.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("fecha_actualizacion") && !itemPS_APROBACION.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("fecha_actualizacion").ToString()) ? (itemPS_APROBACION.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_APROBACION.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_APROBACION.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("usuario_modificacion") && !itemPS_APROBACION.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("usuario_modificacion").ToString()) ? (itemPS_APROBACION.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_APROBACION.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_APROBACION.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("id_movimiento") && !itemPS_APROBACION.GetValue("id_movimiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("id_movimiento").ToString()) ? (itemPS_APROBACION.GetValue("id_movimiento").ToString().Length > 30 ? itemPS_APROBACION.GetValue("id_movimiento").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("id_movimiento").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROBACION.Contains("estado_inicial") && !itemPS_APROBACION.GetValue("estado_inicial").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("estado_inicial").ToString()) ? (itemPS_APROBACION.GetValue("estado_inicial").ToString().Length > 30 ? itemPS_APROBACION.GetValue("estado_inicial").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("estado_inicial").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROBACION.Contains("id_estado") && !itemPS_APROBACION.GetValue("id_estado").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("id_estado").ToString()) ? (itemPS_APROBACION.GetValue("id_estado").ToString().Length > 30 ? itemPS_APROBACION.GetValue("id_estado").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("id_estado").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROBACION.Contains("estado") && !itemPS_APROBACION.GetValue("estado").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("estado").ToString()) ? (itemPS_APROBACION.GetValue("estado").ToString().Length > 30 ? itemPS_APROBACION.GetValue("estado").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("estado").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROBACION.Contains("id_aprovisionamiento") && !itemPS_APROBACION.GetValue("id_aprovisionamiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("id_aprovisionamiento").ToString()) ? (itemPS_APROBACION.GetValue("id_aprovisionamiento").ToString().Length > 30 ? itemPS_APROBACION.GetValue("id_aprovisionamiento").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("id_aprovisionamiento").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROBACION.Contains("numero_orden") && !itemPS_APROBACION.GetValue("numero_orden").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("numero_orden").ToString()) ? (itemPS_APROBACION.GetValue("numero_orden").ToString().Length > 30 ? itemPS_APROBACION.GetValue("numero_orden").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("numero_orden").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROBACION.Contains("id_tipo_aprobacion") && !itemPS_APROBACION.GetValue("id_tipo_aprobacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("id_tipo_aprobacion").ToString()) ? (itemPS_APROBACION.GetValue("id_tipo_aprobacion").ToString().Length > 30 ? itemPS_APROBACION.GetValue("id_tipo_aprobacion").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("id_tipo_aprobacion").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROBACION.Contains("tipo_aprobacion") && !itemPS_APROBACION.GetValue("tipo_aprobacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("tipo_aprobacion").ToString()) ? (itemPS_APROBACION.GetValue("tipo_aprobacion").ToString().Length > 30 ? itemPS_APROBACION.GetValue("tipo_aprobacion").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("tipo_aprobacion").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROBACION.Contains("fecha_solicitud") && !itemPS_APROBACION.GetValue("fecha_solicitud").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("fecha_solicitud").ToString()) ? (itemPS_APROBACION.GetValue("fecha_solicitud").ToString().Length > 30 ? itemPS_APROBACION.GetValue("fecha_solicitud").ToString().Substring(0, 30) : itemPS_APROBACION.GetValue("fecha_solicitud").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("usuario_solicitud") && !itemPS_APROBACION.GetValue("usuario_solicitud").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("usuario_solicitud").ToString()) ? (itemPS_APROBACION.GetValue("usuario_solicitud").ToString().Length > 50 ? itemPS_APROBACION.GetValue("usuario_solicitud").ToString().Substring(0, 50) : itemPS_APROBACION.GetValue("usuario_solicitud").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("observacion") && !itemPS_APROBACION.GetValue("observacion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("observacion").ToString()) ? (itemPS_APROBACION.GetValue("observacion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_APROBACION.GetValue("observacion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_APROBACION.GetValue("observacion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("id_usuario") ? !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("id_usuario")?.ToString()) ? (itemPS_APROBACION.GetValue("id_usuario").ToString().Length > 30 ? itemPS_APROBACION.GetValue("id_usuario").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("id_usuario").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_APROBACION.Contains("texto_alerta") && !itemPS_APROBACION.GetValue("texto_alerta").IsBsonNull ? !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("texto_alerta").ToString()) ? (itemPS_APROBACION.GetValue("texto_alerta").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_APROBACION.GetValue("texto_alerta").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_APROBACION.GetValue("texto_alerta").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("color") && !itemPS_APROBACION.GetValue("color").IsBsonNull ? itemPS_APROBACION.GetValue("color").ToString().Length > 8 ? itemPS_APROBACION.GetValue("color").ToString().Substring(0, 8) : itemPS_APROBACION.GetValue("color").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROBACION.Contains("codigo_verificacion") && !itemPS_APROBACION.GetValue("codigo_verificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROBACION.GetValue("codigo_verificacion").ToString()) ? (itemPS_APROBACION.GetValue("codigo_verificacion").ToString().Length > 30 ? itemPS_APROBACION.GetValue("codigo_verificacion").ToString().Substring(0, 29) : itemPS_APROBACION.GetValue("codigo_verificacion").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                           
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROBACION Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_APROBACION.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS_APROBACION.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROBACION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_APROBACION++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_APROBACION.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS_APROBACION.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROBACION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_APROBACION++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_APROBACION ACTUALIZADA: " + itemPS_APROBACION.GetValue("_id").ToString() + "Numero de PS_APROBACION actializadas: " + Conteo_PS_APROBACION);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROBACION en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROBACION para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_APROBACION > 0)
                        {
                            Archivo_PS_APROBACION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_APROBACION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROBACION entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_APROBACION.Close();
            }

        }

        internal static void Extractor_PS_APROBACION_Usuario(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROBACION_Usuario");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROBACION = null;

            int Conteo_PS_APROBACION_Usuario = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_APROBACION_Usuario_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROBACION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROBACION = db.GetCollection<BsonDocument>("PS_APROBACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROBACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROBACION = builderPS_APROBACION.Empty;

                if (tipo == "")
                {
                    filterPS_APROBACION = builderPS_APROBACION.Or(builderPS_APROBACION.Eq("usuario_aprobacion.Actualizacion_Extractor", "1"), !builderPS_APROBACION.Exists("usuario_aprobacion.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);                    
                    filterPS_APROBACION = builderPS_APROBACION.And(builderPS_APROBACION.Gte("usuario_aprobacion.Fecha_extraccion", fechaconsulta.Date), builderPS_APROBACION.Lt("usuario_aprobacion.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_APROBACION = Col_PS_APROBACION.Find(filterPS_APROBACION).ToList();

                if (consulta_PS_APROBACION != null && consulta_PS_APROBACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROBACION_Usuario encontrados " + consulta_PS_APROBACION.Count.ToString());
                        foreach (BsonDocument itemPS_APROBACION in consulta_PS_APROBACION)
                        {
                            id_mongo = itemPS_APROBACION.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_APROBACION_Usuario = itemPS_APROBACION.GetElement("usuario_aprobacion").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_APROBACION_Usuario != null && consulta_PS_APROBACION_Usuario.Count() > 0)
                            {
                                foreach (BsonValue itemAprobacionUsuario in consulta_PS_APROBACION_Usuario)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemAprobacionUsuario.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemAprobacionUsuario.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemAprobacionUsuario.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_APROBACION.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROBACION.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_APROBACION.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_APROBACION.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_APROBACION.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionUsuario.ToBsonDocument().Contains("usuario_aprobacion") && !itemAprobacionUsuario.ToBsonDocument().GetValue("usuario_aprobacion").IsBsonNull && !string.IsNullOrEmpty(itemAprobacionUsuario.ToBsonDocument().GetValue("usuario_aprobacion").ToString()) ? (itemAprobacionUsuario.ToBsonDocument().GetValue("usuario_aprobacion").ToString().Length > 50 ? itemAprobacionUsuario.ToBsonDocument().GetValue("usuario_aprobacion").ToString().Substring(0, 50) : itemAprobacionUsuario.ToBsonDocument().GetValue("usuario_aprobacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionUsuario.ToBsonDocument().Contains("fecha_aprobacion") && !itemAprobacionUsuario.ToBsonDocument().GetValue("fecha_aprobacion").IsBsonNull && !string.IsNullOrEmpty(itemAprobacionUsuario.ToBsonDocument().GetValue("fecha_aprobacion").ToString()) ? (itemAprobacionUsuario.ToBsonDocument().GetValue("fecha_aprobacion").ToString().Length > 30 ? itemAprobacionUsuario.ToBsonDocument().GetValue("fecha_aprobacion").ToString().Substring(0, 30) : itemAprobacionUsuario.ToBsonDocument().GetValue("fecha_aprobacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionUsuario.ToBsonDocument().Contains("rol_aprobacion") && !itemAprobacionUsuario.ToBsonDocument().GetValue("rol_aprobacion").IsBsonNull && !string.IsNullOrEmpty(itemAprobacionUsuario.ToBsonDocument().GetValue("rol_aprobacion").ToString()) ? (itemAprobacionUsuario.ToBsonDocument().GetValue("rol_aprobacion").ToString().Length > 30 ? itemAprobacionUsuario.ToBsonDocument().GetValue("rol_aprobacion").ToString().Substring(0, 29) : itemAprobacionUsuario.ToBsonDocument().GetValue("rol_aprobacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemAprobacionUsuario.ToBsonDocument().Contains("es_aprobado") && !itemAprobacionUsuario.ToBsonDocument().GetValue("es_aprobado").IsBsonNull ? itemAprobacionUsuario.ToBsonDocument().GetValue("es_aprobado").ToString().Length > 8 ? itemAprobacionUsuario.ToBsonDocument().GetValue("es_aprobado").ToString().Substring(0, 8) : itemAprobacionUsuario.ToBsonDocument().GetValue("es_aprobado").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionUsuario.ToBsonDocument().Contains("firma") && !itemAprobacionUsuario.ToBsonDocument().GetValue("firma").IsBsonNull ? !string.IsNullOrEmpty(itemAprobacionUsuario.ToBsonDocument().GetValue("firma").ToString()) ? (itemAprobacionUsuario.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 660000 ? itemAprobacionUsuario.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemAprobacionUsuario.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : ""); // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,

                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROBACION_Usuario Id: " + id_mongo +","+ itemAprobacionUsuario.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_APROBACION.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_APROBACION.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROBACION.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("usuario_aprobacion.usuario_aprobacion",
                                                                   itemAprobacionUsuario.ToBsonDocument().GetValue("usuario_aprobacion").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("usuario_aprobacion.Actualizacion_Extractor", "0")
                                                                                                .Set("usuario_aprobacion.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_APROBACION_Usuario++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemAprobacionUsuario.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_APROBACION.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                    Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROBACION.GetValue("_id").ToString())), 
                                                                    Builders<BsonDocument>.Filter.Eq("usuario_aprobacion.usuario_aprobacion", 
                                                                    itemAprobacionUsuario.ToBsonDocument().GetValue("usuario_aprobacion").ToString())), 
                                                                    Builders<BsonDocument>.Update.Set("usuario_aprobacion.Actualizacion_Extractor", "0")
                                                                                                 .Set("usuario_aprobacion.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_APROBACION_Usuario++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_APROBACION_Usuario ACTUALIZADA: " + itemPS_APROBACION.GetValue("_id").ToString() + "Numero de PS_APROBACION_Usuario actializadas: " + Conteo_PS_APROBACION_Usuario);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROBACION_Usuario en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROBACION_Usuario para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            
                        }

                        if (Conteo_PS_APROBACION_Usuario > 0)
                        {
                            Archivo_PS_APROBACION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_APROBACION_Usuario_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROBACION_Usuario entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_APROBACION.Close();
            }

        }

        internal static void Extractor_PS_APROBACION_Inventario(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROBACION_Inventario");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROBACION = null;

            int Conteo_PS_APROBACION_Inventario = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_APROBACION_Inventario_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROBACION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROBACION = db.GetCollection<BsonDocument>("PS_APROBACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROBACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROBACION = builderPS_APROBACION.Empty;

                if (tipo == "")
                {
                    filterPS_APROBACION = builderPS_APROBACION.Or(builderPS_APROBACION.Eq("items_inventario.Actualizacion_Extractor", "1"), !builderPS_APROBACION.Exists("items_inventario.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_APROBACION = builderPS_APROBACION.And(builderPS_APROBACION.Gte("items_inventario.Fecha_extraccion", fechaconsulta.Date), builderPS_APROBACION.Lt("items_inventario.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_APROBACION = Col_PS_APROBACION.Find(filterPS_APROBACION).ToList();

                if (consulta_PS_APROBACION != null && consulta_PS_APROBACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROBACION_Inventario encontrados " + consulta_PS_APROBACION.Count.ToString());
                        foreach (BsonDocument itemPS_APROBACION in consulta_PS_APROBACION)
                        {
                            id_mongo = itemPS_APROBACION.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_APROBACION_Usuario = itemPS_APROBACION.GetElement("items_inventario").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_APROBACION_Usuario != null && consulta_PS_APROBACION_Usuario.Count() > 0)
                            {
                                foreach (BsonValue itemAprobacionInventario in consulta_PS_APROBACION_Usuario)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemAprobacionInventario.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemAprobacionInventario.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemAprobacionInventario.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_APROBACION.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROBACION.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_APROBACION.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_APROBACION.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_APROBACION.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionInventario.ToBsonDocument().Contains("id_inventario") && !itemAprobacionInventario.ToBsonDocument().GetValue("id_inventario").IsBsonNull && !string.IsNullOrEmpty(itemAprobacionInventario.ToBsonDocument().GetValue("id_inventario").ToString()) ? (itemAprobacionInventario.ToBsonDocument().GetValue("id_inventario").ToString().Length > 30 ? itemAprobacionInventario.ToBsonDocument().GetValue("id_inventario").ToString().Substring(0, 29) : itemAprobacionInventario.ToBsonDocument().GetValue("id_inventario").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionInventario.ToBsonDocument().Contains("producto_inventario") && !itemAprobacionInventario.ToBsonDocument().GetValue("producto_inventario").IsBsonNull && !string.IsNullOrEmpty(itemAprobacionInventario.ToBsonDocument().GetValue("producto_inventario").ToString()) ? (itemAprobacionInventario.ToBsonDocument().GetValue("producto_inventario").ToString().Length > 50 ? itemAprobacionInventario.ToBsonDocument().GetValue("producto_inventario").ToString().Substring(0, 50) : itemAprobacionInventario.ToBsonDocument().GetValue("producto_inventario").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionInventario.ToBsonDocument().Contains("fecha_aprobacion") && !itemAprobacionInventario.ToBsonDocument().GetValue("fecha_aprobacion").IsBsonNull && !string.IsNullOrEmpty(itemAprobacionInventario.ToBsonDocument().GetValue("fecha_aprobacion").ToString()) ? (itemAprobacionInventario.ToBsonDocument().GetValue("fecha_aprobacion").ToString().Length > 30 ? itemAprobacionInventario.ToBsonDocument().GetValue("fecha_aprobacion").ToString().Substring(0, 30) : itemAprobacionInventario.ToBsonDocument().GetValue("fecha_aprobacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionInventario.ToBsonDocument().Contains("rol_aprobacion") && !itemAprobacionInventario.ToBsonDocument().GetValue("rol_aprobacion").IsBsonNull && !string.IsNullOrEmpty(itemAprobacionInventario.ToBsonDocument().GetValue("rol_aprobacion").ToString()) ? (itemAprobacionInventario.ToBsonDocument().GetValue("rol_aprobacion").ToString().Length > 30 ? itemAprobacionInventario.ToBsonDocument().GetValue("rol_aprobacion").ToString().Substring(0, 29) : itemAprobacionInventario.ToBsonDocument().GetValue("rol_aprobacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemAprobacionInventario.ToBsonDocument().Contains("es_aprobado") && !itemAprobacionInventario.ToBsonDocument().GetValue("es_aprobado").IsBsonNull ? itemAprobacionInventario.ToBsonDocument().GetValue("es_aprobado").ToString().Length > 8 ? itemAprobacionInventario.ToBsonDocument().GetValue("es_aprobado").ToString().Substring(0, 8) : itemAprobacionInventario.ToBsonDocument().GetValue("es_aprobado").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAprobacionInventario.ToBsonDocument().Contains("firma") && !itemAprobacionInventario.ToBsonDocument().GetValue("firma").IsBsonNull ? !string.IsNullOrEmpty(itemAprobacionInventario.ToBsonDocument().GetValue("firma").ToString()) ? (itemAprobacionInventario.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 660000 ? itemAprobacionInventario.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemAprobacionInventario.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : ""); // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,

                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROBACION_Inventario Id: " + id_mongo + "," + itemAprobacionInventario.ToBsonDocument().GetValue("id_inventario").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_APROBACION.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_APROBACION.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROBACION.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("items_inventario.id_inventario",
                                                                   itemAprobacionInventario.ToBsonDocument().GetValue("id_inventario").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("items_inventario.Actualizacion_Extractor", "0")
                                                                                                .Set("items_inventario.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_APROBACION_Inventario++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemAprobacionInventario.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_APROBACION.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                  Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROBACION.GetValue("_id").ToString())),
                                                                  Builders<BsonDocument>.Filter.Eq("items_inventario.id_inventario",
                                                                  itemAprobacionInventario.ToBsonDocument().GetValue("id_inventario").ToString())),
                                                                  Builders<BsonDocument>.Update.Set("items_inventario.Actualizacion_Extractor", "0")
                                                                                               .Set("items_inventario.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_APROBACION_Inventario++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_APROBACION_Inventario ACTUALIZADA: " + itemPS_APROBACION.GetValue("_id").ToString() + "Numero de PS_APROBACION_Inventario actializadas: " + Conteo_PS_APROBACION_Inventario);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROBACION_Inventario en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROBACION_Inventario para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_APROBACION_Inventario > 0)
                        {
                            Archivo_PS_APROBACION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_APROBACION_Inventario_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROBACION_Inventario entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_APROBACION.Close();
            }

        }

        internal static void Extractor_PS_APROVISIONAMIENTO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROVISIONAMIENTO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROVISIONAMIENTO = null;

            int Conteo_PS_APROVISIONAMIENTO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;
            int comunicaciones = 0;
            int historico = 0;
            int servicios_cliente = 0;

            string archivo = path + "PS_APROVISIONAMIENTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROVISIONAMIENTO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROVISIONAMIENTO = db.GetCollection<BsonDocument>("PS_APROVISIONAMIENTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROVISIONAMIENTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROBACION = builderPS_APROVISIONAMIENTO.Empty;

                if (tipo == "")
                {
                    filterPS_APROBACION = builderPS_APROVISIONAMIENTO.Or(builderPS_APROVISIONAMIENTO.Eq("Actualizacion_Extractor", "1"), !builderPS_APROVISIONAMIENTO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);                    
                    filterPS_APROBACION = builderPS_APROVISIONAMIENTO.And(builderPS_APROVISIONAMIENTO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_APROVISIONAMIENTO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_APROVISIONAMIENTO = Col_PS_APROVISIONAMIENTO.Find(filterPS_APROBACION).ToList();

                if (consulta_PS_APROVISIONAMIENTO != null && consulta_PS_APROVISIONAMIENTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROVISIONAMIENTO encontrados " + consulta_PS_APROVISIONAMIENTO.Count.ToString());
                        foreach (BsonDocument itemPS_APROVISIONAMIENTO in consulta_PS_APROVISIONAMIENTO)
                        {
                            id_mongo = itemPS_APROVISIONAMIENTO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_APROVISIONAMIENTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_APROVISIONAMIENTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_APROVISIONAMIENTO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("_id")?.ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("_id").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("_id").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("fecha_creacion") && !itemPS_APROVISIONAMIENTO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("fecha_creacion").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_APROVISIONAMIENTO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("usuario_creacion") && !itemPS_APROVISIONAMIENTO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("usuario_creacion").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_APROVISIONAMIENTO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_APROVISIONAMIENTO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("fecha_actualizacion") && !itemPS_APROVISIONAMIENTO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_APROVISIONAMIENTO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("usuario_modificacion") && !itemPS_APROVISIONAMIENTO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("usuario_modificacion").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_APROVISIONAMIENTO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_APROVISIONAMIENTO.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("tipo_solicitud") && !itemPS_APROVISIONAMIENTO.GetValue("tipo_solicitud").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("tipo_solicitud").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("tipo_solicitud").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("tipo_solicitud").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("tipo_solicitud").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("id_aprovisionamiento") && !itemPS_APROVISIONAMIENTO.GetValue("id_aprovisionamiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("id_aprovisionamiento").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("id_aprovisionamiento").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("id_aprovisionamiento").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("id_aprovisionamiento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("viabilidad_origen") && !itemPS_APROVISIONAMIENTO.GetValue("viabilidad_origen").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("viabilidad_origen").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("viabilidad_origen").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("viabilidad_origen").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("viabilidad_origen").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("opcion_viabilidad") && !itemPS_APROVISIONAMIENTO.GetValue("opcion_viabilidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("opcion_viabilidad").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("opcion_viabilidad").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("opcion_viabilidad").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("opcion_viabilidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("comentarios") && !itemPS_APROVISIONAMIENTO.GetValue("comentarios").IsBsonNull ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("comentarios").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_APROVISIONAMIENTO.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_APROVISIONAMIENTO.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("numero_contrato") ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("numero_contrato")?.ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("numero_contrato").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("numero_contrato").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("numero_contrato").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("fecha_inicio_contrato") && !itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_contrato").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_contrato").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_contrato").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_contrato").ToString().Substring(0, 30) : itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_contrato").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("fecha_fin_contrato") && !itemPS_APROVISIONAMIENTO.GetValue("fecha_fin_contrato").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("fecha_fin_contrato").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("fecha_fin_contrato").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("fecha_fin_contrato").ToString().Substring(0, 30) : itemPS_APROVISIONAMIENTO.GetValue("fecha_fin_contrato").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("renovacion_automatica") && !itemPS_APROVISIONAMIENTO.GetValue("renovacion_automatica").IsBsonNull ? itemPS_APROVISIONAMIENTO.GetValue("renovacion_automatica").ToString().Length > 8 ? itemPS_APROVISIONAMIENTO.GetValue("renovacion_automatica").ToString().Substring(0, 8) : itemPS_APROVISIONAMIENTO.GetValue("renovacion_automatica").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("datos_adicionales_aprovisionamiento") && !itemPS_APROVISIONAMIENTO.GetValue("datos_adicionales_aprovisionamiento").IsBsonNull ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("datos_adicionales_aprovisionamiento").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("datos_adicionales_aprovisionamiento").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_APROVISIONAMIENTO.GetValue("datos_adicionales_aprovisionamiento").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_APROVISIONAMIENTO.GetValue("datos_adicionales_aprovisionamiento").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("id_usuario_asignado") ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("id_usuario_asignado")?.ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("id_usuario_asignado").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("id_usuario_asignado").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("id_usuario_asignado").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("usuario_asignado") && !itemPS_APROVISIONAMIENTO.GetValue("usuario_asignado").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("usuario_asignado").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("usuario_asignado").ToString().Length > 50 ? itemPS_APROVISIONAMIENTO.GetValue("usuario_asignado").ToString().Substring(0, 50) : itemPS_APROVISIONAMIENTO.GetValue("usuario_asignado").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("id_grupo_asignado") && !itemPS_APROVISIONAMIENTO.GetValue("id_grupo_asignado").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("id_grupo_asignado").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("id_grupo_asignado").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("id_grupo_asignado").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("id_grupo_asignado").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("grupo_asignado") && !itemPS_APROVISIONAMIENTO.GetValue("grupo_asignado").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("grupo_asignado").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("grupo_asignado").ToString().Length > 50 ? itemPS_APROVISIONAMIENTO.GetValue("grupo_asignado").ToString().Substring(0, 50) : itemPS_APROVISIONAMIENTO.GetValue("grupo_asignado").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("id_fase") && !itemPS_APROVISIONAMIENTO.GetValue("id_fase").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("id_fase").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("id_fase").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("id_fase").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("id_fase").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("fase_actual") && !itemPS_APROVISIONAMIENTO.GetValue("fase_actual").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("fase_actual").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("fase_actual").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("fase_actual").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("fase_actual").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("id_estado") && !itemPS_APROVISIONAMIENTO.GetValue("id_estado").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("id_estado").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("id_estado").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("id_estado").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("id_estado").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("estado") && !itemPS_APROVISIONAMIENTO.GetValue("estado").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("estado").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("estado").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("estado").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("estado").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("tiempo_ans") && !itemPS_APROVISIONAMIENTO.GetValue("tiempo_ans").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("tiempo_ans").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("tiempo_ans").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("tiempo_ans").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("tiempo_ans").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("id_tipo_proceso") && !itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("tipo_proceso") && !itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("id_tipo_proceso").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("fecha_inicio_facturacion") && !itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_facturacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_facturacion").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_facturacion").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_facturacion").ToString().Substring(0, 30) : itemPS_APROVISIONAMIENTO.GetValue("fecha_inicio_facturacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("solicitud_entrega_materiales") && !itemPS_APROVISIONAMIENTO.GetValue("solicitud_entrega_materiales").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("solicitud_entrega_materiales").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("solicitud_entrega_materiales").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("solicitud_entrega_materiales").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.GetValue("solicitud_entrega_materiales").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_APROVISIONAMIENTO.Contains("fecha_finalizacion") && !itemPS_APROVISIONAMIENTO.GetValue("fecha_finalizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.GetValue("fecha_finalizacion").ToString()) ? (itemPS_APROVISIONAMIENTO.GetValue("fecha_finalizacion").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.GetValue("fecha_finalizacion").ToString().Substring(0, 30) : itemPS_APROVISIONAMIENTO.GetValue("fecha_finalizacion").ToString()) : ""); // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        comunicaciones += Extractor_PS_APROVISIONAMIENTO_Comunicaciones(id_mongo);
                                        historico += Extractor_PS_APROVISIONAMIENTO_historico_estados(id_mongo);
                                        servicios_cliente +=Extractor_PS_APROVISIONAMIENTO_servicios_cliente(id_mongo);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROVISIONAMIENTO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_APROVISIONAMIENTO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS_APROVISIONAMIENTO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROVISIONAMIENTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_APROVISIONAMIENTO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_APROVISIONAMIENTO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS_APROVISIONAMIENTO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROVISIONAMIENTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_APROVISIONAMIENTO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_APROVISIONAMIENTO ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO actializadas: " + Conteo_PS_APROVISIONAMIENTO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROVISIONAMIENTO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROVISIONAMIENTO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_APROVISIONAMIENTO > 0)
                        {
                            Archivo_PS_APROVISIONAMIENTO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_APROVISIONAMIENTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                if (comunicaciones > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_APROVISIONAMIENTO_Comunicaciones_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                                if (historico > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_APROVISIONAMIENTO_historico_estados_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                                if (servicios_cliente > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_APROVISIONAMIENTO_servicios_cliente_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROVISIONAMIENTO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_APROVISIONAMIENTO.Close();
            }

        }

        internal static int Extractor_PS_APROVISIONAMIENTO_Comunicaciones(string id_mongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROVISIONAMIENTO_Comunicaciones");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROVISIONAMIENTO_Comunicaciones = null;

            int Conteo_PS_APROVISIONAMIENTO_Comunicaciones = 0;
            string sTextoDescarga = "";            
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            

            string archivo = path + "PS_APROVISIONAMIENTO_Comunicaciones_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROVISIONAMIENTO_Comunicaciones = new StreamWriter(archivo, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROVISIONAMIENTO = db.GetCollection<BsonDocument>("PS_APROVISIONAMIENTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROVISIONAMIENTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.Empty;
                filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.And(
                builderPS_APROVISIONAMIENTO.Eq("_id", MongoDB.Bson.ObjectId.Parse(id_mongo)),
                builderPS_APROVISIONAMIENTO.SizeGte("comunicaciones", 1));
                

                List<BsonDocument> consulta_PS_APROVISIONAMIENTO = Col_PS_APROVISIONAMIENTO.Find(filterPS_APROVISIONAMIENTO).ToList();

                if (consulta_PS_APROVISIONAMIENTO != null && consulta_PS_APROVISIONAMIENTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROVISIONAMIENTO_Comunicaciones encontrados " + consulta_PS_APROVISIONAMIENTO.Count.ToString());
                        foreach (BsonDocument itemPS_APROVISIONAMIENTO in consulta_PS_APROVISIONAMIENTO)
                        {
                            id_mongo = itemPS_APROVISIONAMIENTO.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_APROVISIONAMIENTO_Comunicaciones = itemPS_APROVISIONAMIENTO.GetElement("comunicaciones").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_APROVISIONAMIENTO_Comunicaciones != null && consulta_PS_APROVISIONAMIENTO_Comunicaciones.Count() > 0)
                            {
                                foreach (BsonValue itemPS_APROVISIONAMIENTO_Comunicaciones in consulta_PS_APROVISIONAMIENTO_Comunicaciones)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga =
                                            (itemPS_APROVISIONAMIENTO.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemPS_APROVISIONAMIENTO_Comunicaciones.ToString());// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROVISIONAMIENTO_Comunicaciones Id: " + id_mongo + "," + itemPS_APROVISIONAMIENTO_Comunicaciones.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga != "")
                                            {
                                                Archivo_PS_APROVISIONAMIENTO_Comunicaciones.WriteLine(sTextoDescarga);
                                                Console.WriteLine(sTextoDescarga);
                                                Conteo_PS_APROVISIONAMIENTO_Comunicaciones++;
                                            }
                                            //Console.WriteLine("PS_APROVISIONAMIENTO_Comunicaciones ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO_Comunicaciones actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROVISIONAMIENTO_Comunicaciones en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROVISIONAMIENTO_Comunicaciones para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_APROVISIONAMIENTO_Comunicaciones ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO_Comunicaciones actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                            }

                        }

                        if (Conteo_PS_APROVISIONAMIENTO_Comunicaciones > 0)
                        {
                            Archivo_PS_APROVISIONAMIENTO_Comunicaciones.Close();                                                       
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROVISIONAMIENTO_Comunicaciones entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_APROVISIONAMIENTO_Comunicaciones;
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
                Archivo_PS_APROVISIONAMIENTO_Comunicaciones.Close();
                
            }
            return Conteo_PS_APROVISIONAMIENTO_Comunicaciones;
        } //Se ejecuta con Extractor_PS_APROVISIONAMIENTO()

        internal static void Extractor_PS_APROVISIONAMIENTO_Adjuntos(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROVISIONAMIENTO_Adjuntos");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROBACION = null;

            int Conteo_PS_APROVISIONAMIENTO_Adjuntos = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_APROVISIONAMIENTO_Adjuntos_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROBACION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROVISIONAMIENTO = db.GetCollection<BsonDocument>("PS_APROVISIONAMIENTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROVISIONAMIENTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.Empty;

                if (tipo == "")
                {
                    filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.Or(builderPS_APROVISIONAMIENTO.Eq("adjuntos.Actualizacion_Extractor", "1"), !builderPS_APROVISIONAMIENTO.Exists("adjuntos.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.And(builderPS_APROVISIONAMIENTO.Gte("adjuntos.Fecha_extraccion", fechaconsulta.Date), builderPS_APROVISIONAMIENTO.Lt("adjuntos.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_APROVISIONAMIENTO = Col_PS_APROVISIONAMIENTO.Find(filterPS_APROVISIONAMIENTO).ToList();

                if (consulta_PS_APROVISIONAMIENTO != null && consulta_PS_APROVISIONAMIENTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROVISIONAMIENTO_Adjuntos encontrados " + consulta_PS_APROVISIONAMIENTO.Count.ToString());
                        foreach (BsonDocument itemPS_APROVISIONAMIENTO in consulta_PS_APROVISIONAMIENTO)
                        {
                            id_mongo = itemPS_APROVISIONAMIENTO.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_APROVISIONAMIENTO_Adjuntos = itemPS_APROVISIONAMIENTO.GetElement("adjuntos").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_APROVISIONAMIENTO_Adjuntos != null && consulta_PS_APROVISIONAMIENTO_Adjuntos.Count() > 0)
                            {
                                foreach (BsonValue itemAPROVISIONAMIENTOAdjuntos in consulta_PS_APROVISIONAMIENTO_Adjuntos)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_APROVISIONAMIENTO.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("_id") && !itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("_id").ToString()) ? (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("ruta") && !itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("ruta").IsBsonNull && !string.IsNullOrEmpty(itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("ruta").ToString()) ? (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("ruta").ToString().Length > 30 ? itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("ruta").ToString().Substring(0, 29) : itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("ruta").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("fecha_creacion") && !itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("usuario_modificacion") && !itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("id_tarea_relacionada") && !itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("id_tarea_relacionada").IsBsonNull && !string.IsNullOrEmpty(itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("id_tarea_relacionada").ToString()) ? (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("id_tarea_relacionada").ToString().Length > 30 ? itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("id_tarea_relacionada").ToString().Substring(0, 29) : itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("id_tarea_relacionada").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("tarea_relacionada") && !itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("tarea_relacionada").IsBsonNull && !string.IsNullOrEmpty(itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("tarea_relacionada").ToString()) ? (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("tarea_relacionada").ToString().Length > 30 ? itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("tarea_relacionada").ToString().Substring(0, 29) : itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("tarea_relacionada").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().Contains("es_publico") && !itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("es_publico").IsBsonNull ? itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("es_publico").ToString().Length > 8 ? itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("es_publico").ToString().Substring(0, 8) : itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("es_publico").ToString() : "");

                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROVISIONAMIENTO_Adjuntos Id: " + id_mongo + "," + itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_APROBACION.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_APROVISIONAMIENTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROVISIONAMIENTO.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("adjuntos._id", MongoDB.Bson.ObjectId.Parse(itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("_id").ToString()))),
                                                                   Builders<BsonDocument>.Update.Set("adjuntos.Actualizacion_Extractor", "0")
                                                                                                .Set("adjuntos.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_APROVISIONAMIENTO_Adjuntos++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_APROVISIONAMIENTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                  Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROVISIONAMIENTO.GetValue("_id").ToString())),
                                                                  Builders<BsonDocument>.Filter.Eq("adjuntos._id", MongoDB.Bson.ObjectId.Parse(itemAPROVISIONAMIENTOAdjuntos.ToBsonDocument().GetValue("_id").ToString()))),
                                                                  Builders<BsonDocument>.Update.Set("adjuntos.Actualizacion_Extractor", "0")
                                                                                               .Set("adjuntos.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_APROVISIONAMIENTO_Adjuntos++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_APROBACION_Usuario ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO_Adjuntos actializadas: " + Conteo_PS_APROVISIONAMIENTO_Adjuntos);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROVISIONAMIENTO_Adjuntos en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROVISIONAMIENTO_Adjuntos para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_APROVISIONAMIENTO_Adjuntos > 0)
                        {
                            Archivo_PS_APROBACION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_APROVISIONAMIENTO_Adjuntos_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROVISIONAMIENTO_Adjuntos entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_APROBACION.Close();
            }

        }

        internal static int Extractor_PS_APROVISIONAMIENTO_historico_estados(string id_mongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROVISIONAMIENTO_historico_estados");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROVISIONAMIENTO_historico_estados = null;

            int Conteo_PS_APROVISIONAMIENTO_historico_estados = 0;
            string sTextoDescarga = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();


            string archivo = path + "PS_APROVISIONAMIENTO_historico_estados_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROVISIONAMIENTO_historico_estados = new StreamWriter(archivo, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROVISIONAMIENTO = db.GetCollection<BsonDocument>("PS_APROVISIONAMIENTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROVISIONAMIENTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.Empty;
                filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.And(
                builderPS_APROVISIONAMIENTO.Eq("_id", MongoDB.Bson.ObjectId.Parse(id_mongo)),
                builderPS_APROVISIONAMIENTO.SizeGte("historico_estados", 1));


                List<BsonDocument> consulta_PS_APROVISIONAMIENTO = Col_PS_APROVISIONAMIENTO.Find(filterPS_APROVISIONAMIENTO).ToList();

                if (consulta_PS_APROVISIONAMIENTO != null && consulta_PS_APROVISIONAMIENTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROVISIONAMIENTO_historico_estados encontrados " + consulta_PS_APROVISIONAMIENTO.Count.ToString());
                        foreach (BsonDocument itemPS_APROVISIONAMIENTO in consulta_PS_APROVISIONAMIENTO)
                        {
                            id_mongo = itemPS_APROVISIONAMIENTO.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_APROVISIONAMIENTO_historico_estados = itemPS_APROVISIONAMIENTO.GetElement("historico_estados").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_APROVISIONAMIENTO_historico_estados != null && consulta_PS_APROVISIONAMIENTO_historico_estados.Count() > 0)
                            {
                                foreach (BsonValue itemPS_APROVISIONAMIENTO_historico_estados in consulta_PS_APROVISIONAMIENTO_historico_estados)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga =
                                            (itemPS_APROVISIONAMIENTO.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemPS_APROVISIONAMIENTO_historico_estados.ToString());// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");

                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROVISIONAMIENTO_historico_estados Id: " + id_mongo + "," + itemPS_APROVISIONAMIENTO_historico_estados.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga != "")
                                            {
                                                Archivo_PS_APROVISIONAMIENTO_historico_estados.WriteLine(sTextoDescarga);
                                                Console.WriteLine(sTextoDescarga);
                                                Conteo_PS_APROVISIONAMIENTO_historico_estados++;
                                            }
                                            //Console.WriteLine("PS_APROVISIONAMIENTO_historico_estados ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO_historico_estados actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROVISIONAMIENTO_historico_estados en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROVISIONAMIENTO_historico_estados para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_APROVISIONAMIENTO_historico_estados ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO_historico_estados actializadas: " + Conteo_PS_APROVISIONAMIENTO_historico_estados);
                            }

                        }

                        if (Conteo_PS_APROVISIONAMIENTO_historico_estados > 0)
                        {
                            Archivo_PS_APROVISIONAMIENTO_historico_estados.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROVISIONAMIENTO_historico_estados entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_APROVISIONAMIENTO_historico_estados;
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
                Archivo_PS_APROVISIONAMIENTO_historico_estados.Close();

            }
            return Conteo_PS_APROVISIONAMIENTO_historico_estados;
        } //Se ejecuta con Extractor_PS_APROVISIONAMIENTO()

        internal static void Extractor_PS_APROVISIONAMIENTO_Tiempos_Solicitud(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROVISIONAMIENTO_Tiempos_Solicitud");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROBACION = null;

            int Conteo_PS_APROVISIONAMIENTO_Adjuntos = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_APROVISIONAMIENTO_Tiempos_Solicitud_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROBACION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROVISIONAMIENTO = db.GetCollection<BsonDocument>("PS_APROVISIONAMIENTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROVISIONAMIENTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.Empty;

                if (tipo == "")
                {
                    filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.Or(builderPS_APROVISIONAMIENTO.Eq("tiempos_solicitud.Actualizacion_Extractor", "1"), !builderPS_APROVISIONAMIENTO.Exists("tiempos_solicitud.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.And(builderPS_APROVISIONAMIENTO.Gte("tiempos_solicitud.Fecha_extraccion", fechaconsulta.Date), builderPS_APROVISIONAMIENTO.Lt("tiempos_solicitud.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_APROVISIONAMIENTO = Col_PS_APROVISIONAMIENTO.Find(filterPS_APROVISIONAMIENTO).ToList();

                if (consulta_PS_APROVISIONAMIENTO != null && consulta_PS_APROVISIONAMIENTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROVISIONAMIENTO_Tiempos_Solicitud encontrados " + consulta_PS_APROVISIONAMIENTO.Count.ToString());
                        foreach (BsonDocument itemPS_APROVISIONAMIENTO in consulta_PS_APROVISIONAMIENTO)
                        {
                            id_mongo = itemPS_APROVISIONAMIENTO.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_APROVISIONAMIENTO_Tiempos_Solicitud = itemPS_APROVISIONAMIENTO.GetElement("tiempos_solicitud").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_APROVISIONAMIENTO_Tiempos_Solicitud != null && consulta_PS_APROVISIONAMIENTO_Tiempos_Solicitud.Count() > 0)
                            {
                                foreach (BsonValue itemTiempos_solicitud in consulta_PS_APROVISIONAMIENTO_Tiempos_Solicitud)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemTiempos_solicitud.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemTiempos_solicitud.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemTiempos_solicitud.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_APROVISIONAMIENTO.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("_id") && !itemTiempos_solicitud.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("_id").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemTiempos_solicitud.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemTiempos_solicitud.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("fecha_creacion") && !itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("usuario_creacion") && !itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_creacion").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_creacion").ToString().Length > 50 ? itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_creacion").ToString().Substring(0, 50) : itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("fecha_actualizacion") && !itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Length > 30 ? itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemTiempos_solicitud.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("usuario_modificacion") && !itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("nombre") && !itemTiempos_solicitud.ToBsonDocument().GetValue("nombre").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("nombre").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("nombre").ToString().Length > 30 ? itemTiempos_solicitud.ToBsonDocument().GetValue("nombre").ToString().Substring(0, 29) : itemTiempos_solicitud.ToBsonDocument().GetValue("nombre").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("descripcion") && !itemTiempos_solicitud.ToBsonDocument().GetValue("descripcion").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("descripcion").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("descripcion").ToString().Length > 30 ? itemTiempos_solicitud.ToBsonDocument().GetValue("descripcion").ToString().Substring(0, 29) : itemTiempos_solicitud.ToBsonDocument().GetValue("descripcion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("color") && !itemTiempos_solicitud.ToBsonDocument().GetValue("color").IsBsonNull ? itemTiempos_solicitud.ToBsonDocument().GetValue("color").ToString().Length > 8 ? itemTiempos_solicitud.ToBsonDocument().GetValue("color").ToString().Substring(0, 8) : itemTiempos_solicitud.ToBsonDocument().GetValue("color").ToString() : "") +
                                                //"~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("color") && !itemTiempos_solicitud.ToBsonDocument().GetValue("color").IsBsonNull ? itemTiempos_solicitud.ToBsonDocument().GetValue("color").ToString().Length > 8 ? itemTiempos_solicitud.ToBsonDocument().GetValue("color").ToString().Substring(0, 8) : itemTiempos_solicitud.ToBsonDocument().GetValue("color").ToString() : "") +
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("fechainicio") && !itemTiempos_solicitud.ToBsonDocument().GetValue("fechainicio").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("fechainicio").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("fechainicio").ToString().Length > 30 ? itemTiempos_solicitud.ToBsonDocument().GetValue("fechainicio").ToString().Substring(0, 30) : itemTiempos_solicitud.ToBsonDocument().GetValue("fechainicio").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("Fechafin") && !itemTiempos_solicitud.ToBsonDocument().GetValue("Fechafin").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("Fechafin").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("Fechafin").ToString().Length > 30 ? itemTiempos_solicitud.ToBsonDocument().GetValue("Fechafin").ToString().Substring(0, 30) : itemTiempos_solicitud.ToBsonDocument().GetValue("Fechafin").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("tiempoTranscurrido") && !itemTiempos_solicitud.ToBsonDocument().GetValue("tiempoTranscurrido").IsBsonNull && !string.IsNullOrEmpty(itemTiempos_solicitud.ToBsonDocument().GetValue("tiempoTranscurrido").ToString()) ? (itemTiempos_solicitud.ToBsonDocument().GetValue("tiempoTranscurrido").ToString().Length > 30 ? itemTiempos_solicitud.ToBsonDocument().GetValue("tiempoTranscurrido").ToString().Substring(0, 29) : itemTiempos_solicitud.ToBsonDocument().GetValue("tiempoTranscurrido").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemTiempos_solicitud.ToBsonDocument().Contains("en_ejecucion") && !itemTiempos_solicitud.ToBsonDocument().GetValue("en_ejecucion").IsBsonNull ? itemTiempos_solicitud.ToBsonDocument().GetValue("en_ejecucion").ToString().Length > 8 ? itemTiempos_solicitud.ToBsonDocument().GetValue("en_ejecucion").ToString().Substring(0, 8) : itemTiempos_solicitud.ToBsonDocument().GetValue("en_ejecucion").ToString() : "");

                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROVISIONAMIENTO_Tiempos_Solicitud Id: " + id_mongo + "," + itemTiempos_solicitud.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_APROBACION.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_APROVISIONAMIENTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROVISIONAMIENTO.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("tiempos_solicitud._id", MongoDB.Bson.ObjectId.Parse(itemTiempos_solicitud.ToBsonDocument().GetValue("_id").ToString()))),
                                                                   Builders<BsonDocument>.Update.Set("tiempos_solicitud.Actualizacion_Extractor", "0")
                                                                                                .Set("tiempos_solicitud.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_APROVISIONAMIENTO_Adjuntos++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemTiempos_solicitud.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_APROVISIONAMIENTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                  Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_APROVISIONAMIENTO.GetValue("_id").ToString())),
                                                                  Builders<BsonDocument>.Filter.Eq("tiempos_solicitud._id", MongoDB.Bson.ObjectId.Parse(itemTiempos_solicitud.ToBsonDocument().GetValue("_id").ToString()))),
                                                                  Builders<BsonDocument>.Update.Set("tiempos_solicitud.Actualizacion_Extractor", "0")
                                                                                               .Set("tiempos_solicitud.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_APROVISIONAMIENTO_Adjuntos++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_APROVISIONAMIENTO_Tiempos_Solicitud ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO_Tiempos_Solicitud actializadas: " + Conteo_PS_APROVISIONAMIENTO_Adjuntos);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROVISIONAMIENTO_Tiempos_Solicitud en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROVISIONAMIENTO_Tiempos_Solicitud para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_APROVISIONAMIENTO_Adjuntos > 0)
                        {
                            Archivo_PS_APROBACION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_APROVISIONAMIENTO_Adjuntos_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROVISIONAMIENTO_Tiempos_Solicitud entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_APROBACION.Close();
            }

        }

        internal static int Extractor_PS_APROVISIONAMIENTO_servicios_cliente(string id_mongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROVISIONAMIENTO_servicios_cliente");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_APROVISIONAMIENTO_historico_estados = null;

            int Conteo_PS_APROVISIONAMIENTO_servicios_cliente = 0;
            string sTextoDescarga = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();


            string archivo = path + "PS_APROVISIONAMIENTO_servicios_cliente_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_APROVISIONAMIENTO_historico_estados = new StreamWriter(archivo, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_APROVISIONAMIENTO = db.GetCollection<BsonDocument>("PS_APROVISIONAMIENTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_APROVISIONAMIENTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.Empty;
                filterPS_APROVISIONAMIENTO = builderPS_APROVISIONAMIENTO.And(
                builderPS_APROVISIONAMIENTO.Eq("_id", MongoDB.Bson.ObjectId.Parse(id_mongo)),
                builderPS_APROVISIONAMIENTO.SizeGte("servicios_cliente", 1));


                List<BsonDocument> consulta_PS_APROVISIONAMIENTO = Col_PS_APROVISIONAMIENTO.Find(filterPS_APROVISIONAMIENTO).ToList();

                if (consulta_PS_APROVISIONAMIENTO != null && consulta_PS_APROVISIONAMIENTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_APROVISIONAMIENTO_servicios_cliente encontrados " + consulta_PS_APROVISIONAMIENTO.Count.ToString());
                        foreach (BsonDocument itemPS_APROVISIONAMIENTO in consulta_PS_APROVISIONAMIENTO)
                        {
                            id_mongo = itemPS_APROVISIONAMIENTO.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_APROVISIONAMIENTO_servicios_cliente = itemPS_APROVISIONAMIENTO.GetElement("servicios_cliente").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_APROVISIONAMIENTO_servicios_cliente != null && consulta_PS_APROVISIONAMIENTO_servicios_cliente.Count() > 0)
                            {
                                foreach (BsonValue itemPS_APROVISIONAMIENTO_servicios_cliente in consulta_PS_APROVISIONAMIENTO_servicios_cliente)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga =
                                            (itemPS_APROVISIONAMIENTO.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_APROVISIONAMIENTO.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemPS_APROVISIONAMIENTO_servicios_cliente.ToString());// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");

                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_APROVISIONAMIENTO_servicios_cliente Id: " + id_mongo + "," + itemPS_APROVISIONAMIENTO_servicios_cliente.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga != "")
                                            {
                                                Archivo_PS_APROVISIONAMIENTO_historico_estados.WriteLine(sTextoDescarga);
                                                Console.WriteLine(sTextoDescarga);
                                                Conteo_PS_APROVISIONAMIENTO_servicios_cliente++;
                                            }
                                            //Console.WriteLine("PS_APROVISIONAMIENTO_servicios_cliente ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO_servicios_cliente actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_APROVISIONAMIENTO_servicios_cliente en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_APROVISIONAMIENTO_servicios_cliente para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_APROVISIONAMIENTO_servicios_cliente ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_APROVISIONAMIENTO_servicios_cliente actializadas: " + Conteo_PS_APROVISIONAMIENTO_servicios_cliente);
                            }

                        }

                        if (Conteo_PS_APROVISIONAMIENTO_servicios_cliente > 0)
                        {
                            Archivo_PS_APROVISIONAMIENTO_historico_estados.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_APROVISIONAMIENTO_servicios_cliente entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_APROVISIONAMIENTO_servicios_cliente;
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
                Archivo_PS_APROVISIONAMIENTO_historico_estados.Close();

            }
            return Conteo_PS_APROVISIONAMIENTO_servicios_cliente;
        } //Se ejecuta con Extractor_PS_APROVISIONAMIENTO()

        internal static void Extractor_PS_ATRIBUTO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_ATRIBUTO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_ATRIBUTO = null;

            int Conteo_PS_ATRIBUTO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_ATRIBUTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_ATRIBUTO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_ATRIBUTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_ATRIBUTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_ATRIBUTO = builderPS_ATRIBUTO.Empty;

                if (tipo == "")
                {
                    filterPS_ATRIBUTO = builderPS_ATRIBUTO.Or(builderPS_ATRIBUTO.Eq("Actualizacion_Extractor", "1"), !builderPS_ATRIBUTO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                   
                    filterPS_ATRIBUTO = builderPS_ATRIBUTO.And(builderPS_ATRIBUTO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_ATRIBUTO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_ATRIBUTO = Col_PS.Find(filterPS_ATRIBUTO).ToList();

                if (consulta_PS_ATRIBUTO != null && consulta_PS_ATRIBUTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_ATRIBUTO encontrados " + consulta_PS_ATRIBUTO.Count.ToString());
                        foreach (BsonDocument itemPS_ATRIBUTO in consulta_PS_ATRIBUTO)
                        {
                            id_mongo = itemPS_ATRIBUTO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_ATRIBUTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_ATRIBUTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_ATRIBUTO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_ATRIBUTO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("_id")?.ToString()) ? (itemPS_ATRIBUTO.GetValue("_id").ToString().Length > 30 ? itemPS_ATRIBUTO.GetValue("_id").ToString().Substring(0, 29) : itemPS_ATRIBUTO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("fecha_creacion") && !itemPS_ATRIBUTO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("fecha_creacion").ToString()) ? (itemPS_ATRIBUTO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_ATRIBUTO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_ATRIBUTO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("usuario_creacion") && !itemPS_ATRIBUTO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("usuario_creacion").ToString()) ? (itemPS_ATRIBUTO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_ATRIBUTO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_ATRIBUTO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("fecha_actualizacion") && !itemPS_ATRIBUTO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_ATRIBUTO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_ATRIBUTO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_ATRIBUTO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("usuario_modificacion") && !itemPS_ATRIBUTO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("usuario_modificacion").ToString()) ? (itemPS_ATRIBUTO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_ATRIBUTO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_ATRIBUTO.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("nombre") ? !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("nombre")?.ToString()) ? (itemPS_ATRIBUTO.GetValue("nombre").ToString().Length > 30 ? itemPS_ATRIBUTO.GetValue("nombre").ToString().Substring(0, 29) : itemPS_ATRIBUTO.GetValue("nombre").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("es_activo") && !itemPS_ATRIBUTO.GetValue("es_activo").IsBsonNull ? itemPS_ATRIBUTO.GetValue("es_activo").ToString().Length > 8 ? itemPS_ATRIBUTO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_ATRIBUTO.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("atributo_sap") && !itemPS_ATRIBUTO.GetValue("atributo_sap").IsBsonNull && !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("atributo_sap").ToString()) ? (itemPS_ATRIBUTO.GetValue("atributo_sap").ToString().Length > 10 ? itemPS_ATRIBUTO.GetValue("atributo_sap").ToString().Substring(0, 10) : itemPS_ATRIBUTO.GetValue("atributo_sap").ToString()) : "") + //VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("descripcion") && !itemPS_ATRIBUTO.GetValue("descripcion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("descripcion").ToString()) ? (itemPS_ATRIBUTO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_ATRIBUTO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_ATRIBUTO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ATRIBUTO.Contains("tipo_dato") && !itemPS_ATRIBUTO.GetValue("tipo_dato").IsBsonNull && !string.IsNullOrEmpty(itemPS_ATRIBUTO.GetValue("tipo_dato").ToString()) ? (itemPS_ATRIBUTO.GetValue("tipo_dato").ToString().Length > 10 ? itemPS_ATRIBUTO.GetValue("tipo_dato").ToString().Substring(0, 10) : itemPS_ATRIBUTO.GetValue("tipo_dato").ToString()) : ""); //VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_ATRIBUTO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_ATRIBUTO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_ATRIBUTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_ATRIBUTO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_ATRIBUTO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_ATRIBUTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_ATRIBUTO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_ATRIBUTO ACTUALIZADA: " + itemPS_ATRIBUTO.GetValue("_id").ToString() + "Numero de PS_ATRIBUTO actializadas: " + Conteo_PS_ATRIBUTO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_ATRIBUTO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_ATRIBUTO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_ATRIBUTO > 0)
                        {
                            Archivo_PS_ATRIBUTO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_ATRIBUTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_ATRIBUTO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_ATRIBUTO.Close();
            }

        }

        internal static void Extractor_PS_BODEGA(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_BODEGA");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_BODEGA = null;

            int Conteo_PS_BODEGA = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_BODEGA_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_BODEGA = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_BODEGA");
                FilterDefinitionBuilder<BsonDocument> builderPS_BODEGA = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_BODEGA = builderPS_BODEGA.Empty;

                if (tipo == "")
                {
                    filterPS_BODEGA = builderPS_BODEGA.Or(builderPS_BODEGA.Eq("Actualizacion_Extractor", "1"), !builderPS_BODEGA.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_BODEGA = builderPS_BODEGA.And(builderPS_BODEGA.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_BODEGA.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_BODEGA = Col_PS.Find(filterPS_BODEGA).ToList();

                if (consulta_PS_BODEGA != null && consulta_PS_BODEGA.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_BODEGA encontrados " + consulta_PS_BODEGA.Count.ToString());
                        foreach (BsonDocument itemPS_BODEGA in consulta_PS_BODEGA)
                        {
                            id_mongo = itemPS_BODEGA.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_BODEGA.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_BODEGA.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_BODEGA.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_BODEGA.Contains("_id") ? !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("_id")?.ToString()) ? (itemPS_BODEGA.GetValue("_id").ToString().Length > 30 ? itemPS_BODEGA.GetValue("_id").ToString().Substring(0, 29) : itemPS_BODEGA.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("nombre_bodega") ? !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("nombre_bodega")?.ToString()) ? (itemPS_BODEGA.GetValue("nombre_bodega").ToString().Length > 30 ? itemPS_BODEGA.GetValue("nombre_bodega").ToString().Substring(0, 29) : itemPS_BODEGA.GetValue("nombre_bodega").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("ubicacion_bodega") ? !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("ubicacion_bodega")?.ToString()) ? (itemPS_BODEGA.GetValue("ubicacion_bodega").ToString().Length > 30 ? itemPS_BODEGA.GetValue("ubicacion_bodega").ToString().Substring(0, 29) : itemPS_BODEGA.GetValue("ubicacion_bodega").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("id_usuario") ? !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("id_usuario")?.ToString()) ? (itemPS_BODEGA.GetValue("id_usuario").ToString().Length > 30 ? itemPS_BODEGA.GetValue("id_usuario").ToString().Substring(0, 29) : itemPS_BODEGA.GetValue("id_usuario").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_BODEGA.Contains("responsable") && !itemPS_BODEGA.GetValue("responsable").IsBsonNull && !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("responsable").ToString()) ? (itemPS_BODEGA.GetValue("responsable").ToString().Length > 50 ? itemPS_BODEGA.GetValue("responsable").ToString().Substring(0, 50) : itemPS_BODEGA.GetValue("responsable").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("contratista") && !itemPS_BODEGA.GetValue("contratista").IsBsonNull && !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("contratista").ToString()) ? (itemPS_BODEGA.GetValue("contratista").ToString().Length > 50 ? itemPS_BODEGA.GetValue("contratista").ToString().Substring(0, 50) : itemPS_BODEGA.GetValue("contratista").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("es_sap") && !itemPS_BODEGA.GetValue("es_sap").IsBsonNull ? itemPS_BODEGA.GetValue("es_sap").ToString().Length > 8 ? itemPS_BODEGA.GetValue("es_sap").ToString().Substring(0, 8) : itemPS_BODEGA.GetValue("es_sap").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("es_activo") && !itemPS_BODEGA.GetValue("es_activo").IsBsonNull ? itemPS_BODEGA.GetValue("es_activo").ToString().Length > 8 ? itemPS_BODEGA.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_BODEGA.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("fecha_creacion") && !itemPS_BODEGA.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("fecha_creacion").ToString()) ? (itemPS_BODEGA.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_BODEGA.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_BODEGA.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("usuario_creacion") && !itemPS_BODEGA.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("usuario_creacion").ToString()) ? (itemPS_BODEGA.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_BODEGA.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_BODEGA.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("fecha_actualizacion") && !itemPS_BODEGA.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("fecha_actualizacion").ToString()) ? (itemPS_BODEGA.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_BODEGA.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_BODEGA.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_BODEGA.Contains("usuario_modificacion") && !itemPS_BODEGA.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_BODEGA.GetValue("usuario_modificacion").ToString()) ? (itemPS_BODEGA.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_BODEGA.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_BODEGA.GetValue("usuario_modificacion").ToString()) : "");// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_BODEGA Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_BODEGA.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_BODEGA.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_BODEGA++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_BODEGA.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_BODEGA.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_BODEGA++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_BODEGA ACTUALIZADA: " + itemPS_BODEGA.GetValue("_id").ToString() + "Numero de PS_BODEGA actializadas: " + Conteo_PS_BODEGA);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_BODEGA en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_BODEGA para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_BODEGA > 0)
                        {
                            Archivo_PS_BODEGA.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_BODEGA_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_BODEGA entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_BODEGA.Close();
            }

        }

        internal static void Extractor_PS_CAMPO_DINAMICO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_CAMPO_DINAMICO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_CAMPO_DINAMICO = null;

            int Conteo_PS_CAMPO_DINAMICO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_CAMPO_DINAMICO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_CAMPO_DINAMICO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_CAMPO_DINAMICO");
                FilterDefinitionBuilder<BsonDocument> builderPS_CAMPO_DINAMICO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_CAMPO_DINAMICO = builderPS_CAMPO_DINAMICO.Empty;

                if (tipo == "")
                {
                    filterPS_CAMPO_DINAMICO = builderPS_CAMPO_DINAMICO.Or(builderPS_CAMPO_DINAMICO.Eq("Actualizacion_Extractor", "1"), !builderPS_CAMPO_DINAMICO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_CAMPO_DINAMICO = builderPS_CAMPO_DINAMICO.And(builderPS_CAMPO_DINAMICO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_CAMPO_DINAMICO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_CAMPO_DINAMICO = Col_PS.Find(filterPS_CAMPO_DINAMICO).ToList();

                if (consulta_PS_CAMPO_DINAMICO != null && consulta_PS_CAMPO_DINAMICO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_CAMPO_DINAMICO encontrados " + consulta_PS_CAMPO_DINAMICO.Count.ToString());
                        foreach (BsonDocument itemPS_CAMPO_DINAMICO in consulta_PS_CAMPO_DINAMICO)
                        {
                            id_mongo = itemPS_CAMPO_DINAMICO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_CAMPO_DINAMICO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_CAMPO_DINAMICO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_CAMPO_DINAMICO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_CAMPO_DINAMICO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_CAMPO_DINAMICO.GetValue("_id")?.ToString()) ? (itemPS_CAMPO_DINAMICO.GetValue("_id").ToString().Length > 30 ? itemPS_CAMPO_DINAMICO.GetValue("_id").ToString().Substring(0, 29) : itemPS_CAMPO_DINAMICO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CAMPO_DINAMICO.Contains("nombre") ? !string.IsNullOrEmpty(itemPS_CAMPO_DINAMICO.GetValue("nombre")?.ToString()) ? (itemPS_CAMPO_DINAMICO.GetValue("nombre").ToString().Length > 30 ? itemPS_CAMPO_DINAMICO.GetValue("nombre").ToString().Substring(0, 29) : itemPS_CAMPO_DINAMICO.GetValue("nombre").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CAMPO_DINAMICO.Contains("tipo") ? !string.IsNullOrEmpty(itemPS_CAMPO_DINAMICO.GetValue("tipo")?.ToString()) ? (itemPS_CAMPO_DINAMICO.GetValue("tipo").ToString().Length > 30 ? itemPS_CAMPO_DINAMICO.GetValue("tipo").ToString().Substring(0, 29) : itemPS_CAMPO_DINAMICO.GetValue("tipo").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CAMPO_DINAMICO.Contains("es_respuesta") && !itemPS_CAMPO_DINAMICO.GetValue("es_respuesta").IsBsonNull ? itemPS_CAMPO_DINAMICO.GetValue("es_respuesta").ToString().Length > 8 ? itemPS_CAMPO_DINAMICO.GetValue("es_respuesta").ToString().Substring(0, 8) : itemPS_CAMPO_DINAMICO.GetValue("es_respuesta").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CAMPO_DINAMICO.Contains("es_activo") && !itemPS_CAMPO_DINAMICO.GetValue("es_activo").IsBsonNull ? itemPS_CAMPO_DINAMICO.GetValue("es_activo").ToString().Length > 8 ? itemPS_CAMPO_DINAMICO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_CAMPO_DINAMICO.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CAMPO_DINAMICO.Contains("es_inventario") && !itemPS_CAMPO_DINAMICO.GetValue("es_inventario").IsBsonNull ? itemPS_CAMPO_DINAMICO.GetValue("es_inventario").ToString().Length > 8 ? itemPS_CAMPO_DINAMICO.GetValue("es_inventario").ToString().Substring(0, 8) : itemPS_CAMPO_DINAMICO.GetValue("es_inventario").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CAMPO_DINAMICO.Contains("id_lista") ? !string.IsNullOrEmpty(itemPS_CAMPO_DINAMICO.GetValue("id_lista")?.ToString()) ? (itemPS_CAMPO_DINAMICO.GetValue("id_lista").ToString().Length > 30 ? itemPS_CAMPO_DINAMICO.GetValue("id_lista").ToString().Substring(0, 29) : itemPS_CAMPO_DINAMICO.GetValue("id_lista").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_CAMPO_DINAMICO.Contains("apiname") && !itemPS_CAMPO_DINAMICO.GetValue("apiname").IsBsonNull && !string.IsNullOrEmpty(itemPS_CAMPO_DINAMICO.GetValue("apiname").ToString()) ? (itemPS_CAMPO_DINAMICO.GetValue("apiname").ToString().Length > 50 ? itemPS_CAMPO_DINAMICO.GetValue("apiname").ToString().Substring(0, 50) : itemPS_CAMPO_DINAMICO.GetValue("apiname").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CAMPO_DINAMICO.Contains("agrupador") && !itemPS_CAMPO_DINAMICO.GetValue("agrupador").IsBsonNull && !string.IsNullOrEmpty(itemPS_CAMPO_DINAMICO.GetValue("agrupador").ToString()) ? (itemPS_CAMPO_DINAMICO.GetValue("agrupador").ToString().Length > 50 ? itemPS_CAMPO_DINAMICO.GetValue("agrupador").ToString().Substring(0, 50) : itemPS_CAMPO_DINAMICO.GetValue("agrupador").ToString()) : ""); // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        

                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_CAMPO_DINAMICO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_CAMPO_DINAMICO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CAMPO_DINAMICO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_CAMPO_DINAMICO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_CAMPO_DINAMICO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CAMPO_DINAMICO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_CAMPO_DINAMICO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_CAMPO_DINAMICO ACTUALIZADA: " + itemPS_CAMPO_DINAMICO.GetValue("_id").ToString() + "Numero de PS_CAMPO_DINAMICO actializadas: " + Conteo_PS_CAMPO_DINAMICO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_CAMPO_DINAMICO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_CAMPO_DINAMICO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_CAMPO_DINAMICO > 0)
                        {
                            Archivo_PS_CAMPO_DINAMICO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_CAMPO_DINAMICO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_CAMPO_DINAMICO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_CAMPO_DINAMICO.Close();
            }

        }

        internal static void Extractor_PS_COMUNICACION(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_COMUNICACION");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_COMUNICACION = null;

            int Conteo_PS_COMUNICACION = 0;
            int Destinatarios = 0;
            int Adjuntos = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_COMUNICACION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_COMUNICACION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_COMUNICACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_COMUNICACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_COMUNICACION = builderPS_COMUNICACION.Empty;

                if (tipo == "")
                {
                    filterPS_COMUNICACION = builderPS_COMUNICACION.Or(builderPS_COMUNICACION.Eq("Actualizacion_Extractor", "1"), !builderPS_COMUNICACION.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_COMUNICACION = builderPS_COMUNICACION.And(builderPS_COMUNICACION.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_COMUNICACION.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_COMUNICACION = Col_PS.Find(filterPS_COMUNICACION).ToList();

                if (consulta_PS_COMUNICACION != null && consulta_PS_COMUNICACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_COMUNICACION encontrados " + consulta_PS_COMUNICACION.Count.ToString());
                        foreach (BsonDocument itemPS_COMUNICACION in consulta_PS_COMUNICACION)
                        {
                            id_mongo = itemPS_COMUNICACION.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_COMUNICACION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_COMUNICACION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_COMUNICACION.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_COMUNICACION.Contains("_id") ? !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("_id")?.ToString()) ? (itemPS_COMUNICACION.GetValue("_id").ToString().Length > 30 ? itemPS_COMUNICACION.GetValue("_id").ToString().Substring(0, 29) : itemPS_COMUNICACION.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_COMUNICACION.Contains("fecha_creacion") && !itemPS_COMUNICACION.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("fecha_creacion").ToString()) ? (itemPS_COMUNICACION.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_COMUNICACION.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_COMUNICACION.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_COMUNICACION.Contains("usuario_creacion") && !itemPS_COMUNICACION.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("usuario_creacion").ToString()) ? (itemPS_COMUNICACION.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_COMUNICACION.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_COMUNICACION.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_COMUNICACION.Contains("fecha_actualizacion") && !itemPS_COMUNICACION.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("fecha_actualizacion").ToString()) ? (itemPS_COMUNICACION.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_COMUNICACION.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_COMUNICACION.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_COMUNICACION.Contains("usuario_modificacion") && !itemPS_COMUNICACION.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("usuario_modificacion").ToString()) ? (itemPS_COMUNICACION.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_COMUNICACION.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_COMUNICACION.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_COMUNICACION.Contains("asunto") && !itemPS_COMUNICACION.GetValue("asunto").IsBsonNull && !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("asunto").ToString()) ? (itemPS_COMUNICACION.GetValue("asunto").ToString().Length > 50 ? itemPS_COMUNICACION.GetValue("asunto").ToString().Substring(0, 50) : itemPS_COMUNICACION.GetValue("asunto").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_COMUNICACION.Contains("mensaje_html") ? !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("mensaje_html")?.ToString()) ? (itemPS_COMUNICACION.GetValue("mensaje_html").ToString().Length > 30 ? itemPS_COMUNICACION.GetValue("mensaje_html").ToString().Substring(0, 29) : itemPS_COMUNICACION.GetValue("mensaje_html").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_COMUNICACION.Contains("id_tarea_realcionada") ? !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("id_tarea_realcionada")?.ToString()) ? (itemPS_COMUNICACION.GetValue("id_tarea_realcionada").ToString().Length > 30 ? itemPS_COMUNICACION.GetValue("id_tarea_realcionada").ToString().Substring(0, 29) : itemPS_COMUNICACION.GetValue("id_tarea_realcionada").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_COMUNICACION.Contains("tarea_realcionada") && !itemPS_COMUNICACION.GetValue("tarea_realcionada").IsBsonNull && !string.IsNullOrEmpty(itemPS_COMUNICACION.GetValue("tarea_realcionada").ToString()) ? (itemPS_COMUNICACION.GetValue("tarea_realcionada").ToString().Length > 50 ? itemPS_COMUNICACION.GetValue("tarea_realcionada").ToString().Substring(0, 50) : itemPS_COMUNICACION.GetValue("tarea_realcionada").ToString()) : ""); // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");

                                        Destinatarios += Extractor_PS_COMUNICACION_Destinatarios(id_mongo);
                                        Adjuntos += Extractor_PS_COMUNICACION_adjuntos_comunicaciones(id_mongo);

                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_COMUNICACION Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_COMUNICACION.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_COMUNICACION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_COMUNICACION++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_COMUNICACION.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_COMUNICACION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_COMUNICACION++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_COMUNICACION ACTUALIZADA: " + itemPS_COMUNICACION.GetValue("_id").ToString() + "Numero de PS_COMUNICACION actializadas: " + Conteo_PS_COMUNICACION);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_COMUNICACION en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_COMUNICACION para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_COMUNICACION > 0)
                        {
                            Archivo_PS_COMUNICACION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_COMUNICACION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");

                                if (Destinatarios > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_COMUNICACION_Destinatarios_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                                if (Adjuntos > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores(""PS_COMUNICACION_adjuntos_comunicaciones_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }

                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_COMUNICACION entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_COMUNICACION.Close();
            }

        }

        internal static int Extractor_PS_COMUNICACION_Destinatarios(string id_mongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_COMUNICACION_Destinatarios");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_COMUNICACION_Destinatarios = null;

            int Conteo_PS_COMUNICACION_Destinatarios = 0;
            string sTextoDescarga = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();


            string archivo = path + "PS_COMUNICACION_Destinatarios_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_COMUNICACION_Destinatarios = new StreamWriter(archivo, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_COMUNICACION = db.GetCollection<BsonDocument>("PS_COMUNICACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_COMUNICACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_COMUNICACION = builderPS_COMUNICACION.Empty;
                filterPS_COMUNICACION = builderPS_COMUNICACION.And(
                builderPS_COMUNICACION.Eq("_id", MongoDB.Bson.ObjectId.Parse(id_mongo)),
                builderPS_COMUNICACION.SizeGte("destinatarios", 1));


                List<BsonDocument> consulta_PS_COMUNICACION = Col_PS_COMUNICACION.Find(filterPS_COMUNICACION).ToList();

                if (consulta_PS_COMUNICACION != null && consulta_PS_COMUNICACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_COMUNICACION_Destinatarios encontrados " + consulta_PS_COMUNICACION.Count.ToString());
                        foreach (BsonDocument itemPS_COMUNICACION in consulta_PS_COMUNICACION)
                        {
                            id_mongo = itemPS_COMUNICACION.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_COMUNICACION_Destinatarios = itemPS_COMUNICACION.GetElement("destinatarios").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_COMUNICACION_Destinatarios != null && consulta_PS_COMUNICACION_Destinatarios.Count() > 0)
                            {
                                foreach (BsonValue itemPS_COMUNICACION_Destinatarios in consulta_PS_COMUNICACION_Destinatarios)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga =
                                            (itemPS_COMUNICACION.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_COMUNICACION.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_COMUNICACION.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_COMUNICACION.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_COMUNICACION.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemPS_COMUNICACION_Destinatarios.ToString());// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");

                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_COMUNICACION_Destinatarios Id: " + id_mongo + "," + itemPS_COMUNICACION_Destinatarios.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga != "")
                                            {
                                                Archivo_PS_COMUNICACION_Destinatarios.WriteLine(sTextoDescarga);
                                                Console.WriteLine(sTextoDescarga);
                                                Conteo_PS_COMUNICACION_Destinatarios++;
                                            }
                                            //Console.WriteLine("PS_COMUNICACION_Destinatarios ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_COMUNICACION_Destinatarios actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_COMUNICACION_Destinatarios en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_COMUNICACION_Destinatarios para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_COMUNICACION_Destinatarios ACTUALIZADA: " + itemPS_COMUNICACION.GetValue("_id").ToString() + "Numero de PS_COMUNICACION_Destinatarios actializadas: " + Conteo_PS_COMUNICACION_Destinatarios);
                            }

                        }

                        if (Conteo_PS_COMUNICACION_Destinatarios > 0)
                        {
                            Archivo_PS_COMUNICACION_Destinatarios.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_COMUNICACION_Destinatarios entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_COMUNICACION_Destinatarios;
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
                Archivo_PS_COMUNICACION_Destinatarios.Close();

            }
            return Conteo_PS_COMUNICACION_Destinatarios;
        } //Se ejecuta con Extractor_PS_COMUNICACION()

        internal static int Extractor_PS_COMUNICACION_adjuntos_comunicaciones(string id_mongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_COMUNICACION_adjuntos_comunicaciones");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_COMUNICACION_adjuntos_comunicaciones = null;

            int Conteo_PS_COMUNICACION_adjuntos_comunicaciones = 0;
            string sTextoDescarga = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();


            string archivo = path + "PS_COMUNICACION_adjuntos_comunicaciones_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_COMUNICACION_adjuntos_comunicaciones = new StreamWriter(archivo, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_COMUNICACION = db.GetCollection<BsonDocument>("PS_COMUNICACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_COMUNICACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_COMUNICACION = builderPS_COMUNICACION.Empty;
                filterPS_COMUNICACION = builderPS_COMUNICACION.And(
                builderPS_COMUNICACION.Eq("_id", MongoDB.Bson.ObjectId.Parse(id_mongo)),
                builderPS_COMUNICACION.SizeGte("adjuntos_comunicaciones", 1));


                List<BsonDocument> consulta_PS_COMUNICACION = Col_PS_COMUNICACION.Find(filterPS_COMUNICACION).ToList();

                if (consulta_PS_COMUNICACION != null && consulta_PS_COMUNICACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_COMUNICACION_adjuntos_comunicaciones encontrados " + consulta_PS_COMUNICACION.Count.ToString());
                        foreach (BsonDocument itemPS_COMUNICACION in consulta_PS_COMUNICACION)
                        {
                            id_mongo = itemPS_COMUNICACION.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_COMUNICACION_adjuntos_comunicaciones = itemPS_COMUNICACION.GetElement("adjuntos_comunicaciones").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_COMUNICACION_adjuntos_comunicaciones != null && consulta_PS_COMUNICACION_adjuntos_comunicaciones.Count() > 0)
                            {
                                foreach (BsonValue itemPS_COMUNICACION_adjuntos_comunicaciones in consulta_PS_COMUNICACION_adjuntos_comunicaciones)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga =
                                            (itemPS_COMUNICACION.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_COMUNICACION.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_COMUNICACION.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_COMUNICACION.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_COMUNICACION.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemPS_COMUNICACION_adjuntos_comunicaciones.ToString());// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");

                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_COMUNICACION_adjuntos_comunicaciones Id: " + id_mongo + "," + itemPS_COMUNICACION_adjuntos_comunicaciones.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga != "")
                                            {
                                                Archivo_PS_COMUNICACION_adjuntos_comunicaciones.WriteLine(sTextoDescarga);
                                                Console.WriteLine(sTextoDescarga);
                                                Conteo_PS_COMUNICACION_adjuntos_comunicaciones++;
                                            }
                                            //Console.WriteLine("PS_COMUNICACION_adjuntos_comunicaciones ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_COMUNICACION_adjuntos_comunicaciones actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_COMUNICACION_adjuntos_comunicaciones en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_COMUNICACION_adjuntos_comunicaciones para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_COMUNICACION_adjuntos_comunicaciones ACTUALIZADA: " + itemPS_COMUNICACION.GetValue("_id").ToString() + "Numero de PS_COMUNICACION_adjuntos_comunicaciones actializadas: " + Conteo_PS_COMUNICACION_adjuntos_comunicaciones);
                            }

                        }

                        if (Conteo_PS_COMUNICACION_adjuntos_comunicaciones > 0)
                        {
                            Archivo_PS_COMUNICACION_adjuntos_comunicaciones.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_COMUNICACION_adjuntos_comunicaciones entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_COMUNICACION_adjuntos_comunicaciones;
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
                Archivo_PS_COMUNICACION_adjuntos_comunicaciones.Close();

            }
            return Conteo_PS_COMUNICACION_adjuntos_comunicaciones;
        } //Se ejecuta con Extractor_PS_COMUNICACION()

        internal static void Extractor_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = null;

            int Conteo_PS_COMUNICACION = 0;            
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Empty;

                if (tipo == "")
                {
                    filterPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Or(builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Eq("Actualizacion_Extractor", "1"), !builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.And(builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = Col_PS.Find(filterPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO).ToList();

                if (consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO != null && consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO encontrados " + consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Count.ToString());
                        foreach (BsonDocument itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO in consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO)
                        {
                            id_mongo = itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("_id")?.ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("_id").ToString().Length > 30 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("_id").ToString().Substring(0, 29) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("fecha_creacion") && !itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_creacion").ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("usuario_creacion") && !itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_creacion").ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("fecha_actualizacion") && !itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("usuario_modificacion") && !itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_modificacion").ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("id_producto") && !itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("id_producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("id_producto").ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("id_producto").ToString().Length > 50 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("id_producto").ToString().Substring(0, 50) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("id_producto").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("producto") && !itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("producto").ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("producto").ToString().Length > 50 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("producto").ToString().Substring(0, 50) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("producto").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("es_activo") && !itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("es_activo").IsBsonNull ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("es_activo").ToString().Length > 8 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Contains("observaciones") && !itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("observaciones").IsBsonNull ? !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("observaciones").ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "");// VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");                                     

                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_COMUNICACION++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_COMUNICACION++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO ACTUALIZADA: " + itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.GetValue("_id").ToString() + "Numero de PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO actializadas: " + Conteo_PS_COMUNICACION);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_COMUNICACION > 0)
                        {
                            Archivo_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");                               
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Close();
            }

        }

        internal static void Extractor_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo _PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos = null;

            int Conteo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = db.GetCollection<BsonDocument>("PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Empty;

                if (tipo == "")
                {
                    filterPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Or(builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Eq("campos_dinamicos.Actualizacion_Extractor", "1"), !builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Exists("campos_dinamicos.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.And(builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Gte("campos_dinamicos.Fecha_extraccion", fechaconsulta.Date), builderPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Lt("campos_dinamicos.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO = Col_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Find(filterPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO).ToList();

                if (consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO != null && consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos encontrados " + consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.Count.ToString());
                        foreach (BsonDocument itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos in consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO)
                        {
                            id_mongo = itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos = itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.GetElement("campos_dinamicos").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos != null && consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos.Count() > 0)
                            {
                                foreach (BsonValue itemCampos_Dinamicos in consulta_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemCampos_Dinamicos.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemCampos_Dinamicos.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemCampos_Dinamicos.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("_id") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(itemCampos_Dinamicos.ToBsonDocument().GetValue("_id").ToString()) ? (itemCampos_Dinamicos.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemCampos_Dinamicos.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("nombre") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("nombre").IsBsonNull && !string.IsNullOrEmpty(itemCampos_Dinamicos.ToBsonDocument().GetValue("nombre").ToString()) ? (itemCampos_Dinamicos.ToBsonDocument().GetValue("nombre").ToString().Length > 50 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("nombre").ToString().Substring(0, 50) : itemCampos_Dinamicos.ToBsonDocument().GetValue("nombre").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("es_respuesta") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("es_respuesta").IsBsonNull ? itemCampos_Dinamicos.ToBsonDocument().GetValue("es_respuesta").ToString().Length > 8 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("es_respuesta").ToString().Substring(0, 8) : itemCampos_Dinamicos.ToBsonDocument().GetValue("es_respuesta").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("es_activo") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("es_activo").IsBsonNull ? itemCampos_Dinamicos.ToBsonDocument().GetValue("es_activo").ToString().Length > 8 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("es_activo").ToString().Substring(0, 8) : itemCampos_Dinamicos.ToBsonDocument().GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("es_inventario") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("es_inventario").IsBsonNull ? itemCampos_Dinamicos.ToBsonDocument().GetValue("es_inventario").ToString().Length > 8 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("es_inventario").ToString().Substring(0, 8) : itemCampos_Dinamicos.ToBsonDocument().GetValue("es_inventario").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("id_lista") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("id_lista").IsBsonNull && !string.IsNullOrEmpty(itemCampos_Dinamicos.ToBsonDocument().GetValue("id_lista").ToString()) ? (itemCampos_Dinamicos.ToBsonDocument().GetValue("id_lista").ToString().Length > 30 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("id_lista").ToString().Substring(0, 29) : itemCampos_Dinamicos.ToBsonDocument().GetValue("id_lista").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("apiname") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("apiname").IsBsonNull && !string.IsNullOrEmpty(itemCampos_Dinamicos.ToBsonDocument().GetValue("apiname").ToString()) ? (itemCampos_Dinamicos.ToBsonDocument().GetValue("apiname").ToString().Length > 30 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("apiname").ToString().Substring(0, 29) : itemCampos_Dinamicos.ToBsonDocument().GetValue("apiname").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("agrupador") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("agrupador").IsBsonNull && !string.IsNullOrEmpty(itemCampos_Dinamicos.ToBsonDocument().GetValue("agrupador").ToString()) ? (itemCampos_Dinamicos.ToBsonDocument().GetValue("agrupador").ToString().Length > 30 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("agrupador").ToString().Substring(0, 29) : itemCampos_Dinamicos.ToBsonDocument().GetValue("agrupador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemCampos_Dinamicos.ToBsonDocument().Contains("orden_visualizacion") && !itemCampos_Dinamicos.ToBsonDocument().GetValue("orden_visualizador").IsBsonNull && !string.IsNullOrEmpty(itemCampos_Dinamicos.ToBsonDocument().GetValue("orden_visualizador").ToString()) ? (itemCampos_Dinamicos.ToBsonDocument().GetValue("orden_visualizador").ToString().Length > 30 ? itemCampos_Dinamicos.ToBsonDocument().GetValue("orden_visualizador").ToString().Substring(0, 29) : itemCampos_Dinamicos.ToBsonDocument().GetValue("orden_visualizador").ToString()) : "");
                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de _PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos Id: " + id_mongo + "," + itemCampos_Dinamicos.ToBsonDocument().GetValue("id_inventario").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("campos_dinamicos._id", itemCampos_Dinamicos.ToBsonDocument().GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("campos_dinamicos.Actualizacion_Extractor", "0")
                                                                                                .Set("campos_dinamicos.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemCampos_Dinamicos.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("campos_dinamicos._id", itemCampos_Dinamicos.ToBsonDocument().GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("campos_dinamicos.Actualizacion_Extractor", "0")
                                                                                                .Set("campos_dinamicos.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos ACTUALIZADA: " + itemPS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_Campos.GetValue("_id").ToString() + "Numero de _PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos actializadas: " + Conteo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en _PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion _PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos > 0)
                        {
                            Archivo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("_PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en _PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo__PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO_campos_dinamicos.Close();
            }

        }

        internal static void Extractor_PS_CONFIG_SERVICIO_PRODUCTO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_CONFIG_SERVICIO_PRODUCTO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_CONFIG_SERVICIO_PRODUCTO = null;

            int Conteo_PS_CONFIG_SERVICIO_PRODUCTO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;
            

            string archivo = path + "PS_CONFIG_SERVICIO_PRODUCTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_CONFIG_SERVICIO_PRODUCTO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_CONFIG_SERVICIO_PRODUCTO = db.GetCollection<BsonDocument>("PS_CONFIG_SERVICIO_PRODUCTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_CONFIG_SERVICIO_PRODUCTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_CONFIG_SERVICIO_PRODUCTO = builderPS_CONFIG_SERVICIO_PRODUCTO.Empty;

                if (tipo == "")
                {
                    filterPS_CONFIG_SERVICIO_PRODUCTO = builderPS_CONFIG_SERVICIO_PRODUCTO.Or(builderPS_CONFIG_SERVICIO_PRODUCTO.Eq("Actualizacion_Extractor", "1"), !builderPS_CONFIG_SERVICIO_PRODUCTO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_CONFIG_SERVICIO_PRODUCTO = builderPS_CONFIG_SERVICIO_PRODUCTO.And(builderPS_CONFIG_SERVICIO_PRODUCTO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_CONFIG_SERVICIO_PRODUCTO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_CONFIG_SERVICIO_PRODUCTO = Col_PS_CONFIG_SERVICIO_PRODUCTO.Find(filterPS_CONFIG_SERVICIO_PRODUCTO).ToList();

                if (consulta_PS_CONFIG_SERVICIO_PRODUCTO != null && consulta_PS_CONFIG_SERVICIO_PRODUCTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_CONFIG_SERVICIO_PRODUCTO encontrados " + consulta_PS_CONFIG_SERVICIO_PRODUCTO.Count.ToString());
                        foreach (BsonDocument itemPS_CONFIG_SERVICIO_PRODUCTO in consulta_PS_CONFIG_SERVICIO_PRODUCTO)
                        {
                            id_mongo = itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_CONFIG_SERVICIO_PRODUCTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_CONFIG_SERVICIO_PRODUCTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_CONFIG_SERVICIO_PRODUCTO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id")?.ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString().Length > 30 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString().Substring(0, 29) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("fecha_creacion") && !itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_creacion").ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("usuario_creacion") && !itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_creacion").ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("fecha_actualizacion") && !itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("usuario_modificacion") && !itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_modificacion").ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("id_producto") && !itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("id_producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("id_producto").ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("id_producto").ToString().Length > 30 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("id_producto").ToString().Substring(0, 29) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("id_producto").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("producto") && !itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("producto").ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("producto").ToString().Length > 50 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("producto").ToString().Substring(0, 50) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("producto").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("es_activo") && !itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("es_activo").IsBsonNull ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("es_activo").ToString().Length > 8 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONFIG_SERVICIO_PRODUCTO.Contains("observaciones") && !itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("observaciones").IsBsonNull ? !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("observaciones").ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "");

                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_CONFIG_SERVICIO_PRODUCTO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_CONFIG_SERVICIO_PRODUCTO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS_CONFIG_SERVICIO_PRODUCTO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_CONFIG_SERVICIO_PRODUCTO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS_CONFIG_SERVICIO_PRODUCTO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_CONFIG_SERVICIO_PRODUCTO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_CONFIG_SERVICIO_PRODUCTO ACTUALIZADA: " + itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString() + "Numero de PS_CONFIG_SERVICIO_PRODUCTO actializadas: " + Conteo_PS_CONFIG_SERVICIO_PRODUCTO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_CONFIG_SERVICIO_PRODUCTO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_CONFIG_SERVICIO_PRODUCTO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_CONFIG_SERVICIO_PRODUCTO > 0)
                        {
                            Archivo_PS_CONFIG_SERVICIO_PRODUCTO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_CONFIG_SERVICIO_PRODUCTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");

                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_CONFIG_SERVICIO_PRODUCTO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_CONFIG_SERVICIO_PRODUCTO.Close();
            }

        }

        internal static void Extractor_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion = null;

            int Conteo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_CONFIG_SERVICIO_PRODUCTO = db.GetCollection<BsonDocument>("PS_CONFIG_SERVICIO_PRODUCTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_CONFIG_SERVICIO_PRODUCTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_CONFIG_SERVICIO_PRODUCTO = builderPS_CONFIG_SERVICIO_PRODUCTO.Empty;

                if (tipo == "")
                {
                    filterPS_CONFIG_SERVICIO_PRODUCTO = builderPS_CONFIG_SERVICIO_PRODUCTO.Or(builderPS_CONFIG_SERVICIO_PRODUCTO.Eq("elementos_configuracion.Actualizacion_Extractor", "1"), !builderPS_CONFIG_SERVICIO_PRODUCTO.Exists("elementos_configuracion.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_CONFIG_SERVICIO_PRODUCTO = builderPS_CONFIG_SERVICIO_PRODUCTO.And(builderPS_CONFIG_SERVICIO_PRODUCTO.Gte("elementos_configuracion.Fecha_extraccion", fechaconsulta.Date), builderPS_CONFIG_SERVICIO_PRODUCTO.Lt("elementos_configuracion.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_CONFIG_SERVICIO_PRODUCTO = Col_PS_CONFIG_SERVICIO_PRODUCTO.Find(filterPS_CONFIG_SERVICIO_PRODUCTO).ToList();

                if (consulta_PS_CONFIG_SERVICIO_PRODUCTO != null && consulta_PS_CONFIG_SERVICIO_PRODUCTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion encontrados " + consulta_PS_CONFIG_SERVICIO_PRODUCTO.Count.ToString());
                        foreach (BsonDocument itemPS_CONFIG_SERVICIO_PRODUCTO in consulta_PS_CONFIG_SERVICIO_PRODUCTO)
                        {
                            id_mongo = itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion = itemPS_CONFIG_SERVICIO_PRODUCTO.GetElement("elementos_configuracion").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion != null && consulta_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion.Count() > 0)
                            {
                                foreach (BsonValue itemElementos_Configuracion in consulta_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemElementos_Configuracion.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemElementos_Configuracion.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemElementos_Configuracion.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_CONFIG_SERVICIO_PRODUCTO.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_CONFIG_SERVICIO_PRODUCTO.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_CONFIG_SERVICIO_PRODUCTO.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_CONFIG_SERVICIO_PRODUCTO.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_CONFIG_SERVICIO_PRODUCTO.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("_id") && !itemElementos_Configuracion.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(itemElementos_Configuracion.ToBsonDocument().GetValue("_id").ToString()) ? (itemElementos_Configuracion.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemElementos_Configuracion.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemElementos_Configuracion.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("fecha_creacion") && !itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("usuario_creacion") && !itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString()) ? (itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString().Length > 50 ? itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString().Substring(0, 50) : itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("fecha_actualizacion") && !itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) ? (itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Length > 30 ? itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemElementos_Configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("usuario_modificacion") && !itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("id_agrupador") && !itemElementos_Configuracion.ToBsonDocument().GetValue("id_agrupador").IsBsonNull ? itemElementos_Configuracion.ToBsonDocument().GetValue("id_agrupador").ToString().Length > 8 ? itemElementos_Configuracion.ToBsonDocument().GetValue("id_agrupador").ToString().Substring(0, 8) : itemElementos_Configuracion.ToBsonDocument().GetValue("id_agrupador").ToString() : "") +
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("nombre_agrupador") && !itemElementos_Configuracion.ToBsonDocument().GetValue("nombre_agrupador").IsBsonNull && !string.IsNullOrEmpty(itemElementos_Configuracion.ToBsonDocument().GetValue("nombre_agrupador").ToString()) ? (itemElementos_Configuracion.ToBsonDocument().GetValue("nombre_agrupador").ToString().Length > 30 ? itemElementos_Configuracion.ToBsonDocument().GetValue("nombre_agrupador").ToString().Substring(0, 29) : itemElementos_Configuracion.ToBsonDocument().GetValue("nombre_agrupador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("cantidad") && !itemElementos_Configuracion.ToBsonDocument().GetValue("cantidad").IsBsonNull && !string.IsNullOrEmpty(itemElementos_Configuracion.ToBsonDocument().GetValue("cantidad").ToString()) ? (itemElementos_Configuracion.ToBsonDocument().GetValue("cantidad").ToString().Length > 30 ? itemElementos_Configuracion.ToBsonDocument().GetValue("cantidad").ToString().Substring(0, 29) : itemElementos_Configuracion.ToBsonDocument().GetValue("cantidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("tipo_elemento") && !itemElementos_Configuracion.ToBsonDocument().GetValue("tipo_elemento").IsBsonNull ? itemElementos_Configuracion.ToBsonDocument().GetValue("tipo_elemento").ToString().Length > 8 ? itemElementos_Configuracion.ToBsonDocument().GetValue("tipo_elemento").ToString().Substring(0, 8) : itemElementos_Configuracion.ToBsonDocument().GetValue("tipo_elemento").ToString() : "") +
                                                "~|" + (itemElementos_Configuracion.ToBsonDocument().Contains("inventario_etb") && !itemElementos_Configuracion.ToBsonDocument().GetValue("inventario_etb").IsBsonNull ? itemElementos_Configuracion.ToBsonDocument().GetValue("inventario_etb").ToString().Length > 8 ? itemElementos_Configuracion.ToBsonDocument().GetValue("inventario_etb").ToString().Substring(0, 8) : itemElementos_Configuracion.ToBsonDocument().GetValue("inventario_etb").ToString() : "") ;
                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion Id: " + id_mongo + "," + itemElementos_Configuracion.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_CONFIG_SERVICIO_PRODUCTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("elementos_configuracion._id", MongoDB.Bson.ObjectId.Parse(itemElementos_Configuracion.ToBsonDocument().GetValue("_id").ToString()))),
                                                                   Builders<BsonDocument>.Update.Set("elementos_configuracion.Actualizacion_Extractor", "0")
                                                                                                .Set("elementos_configuracion.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemElementos_Configuracion.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_CONFIG_SERVICIO_PRODUCTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                  Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString())),
                                                                  Builders<BsonDocument>.Filter.Eq("elementos_configuracion._id", MongoDB.Bson.ObjectId.Parse(itemElementos_Configuracion.ToBsonDocument().GetValue("_id").ToString()))),
                                                                  Builders<BsonDocument>.Update.Set("elementos_configuracion.Actualizacion_Extractor", "0")
                                                                                               .Set("elementos_configuracion.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion ACTUALIZADA: " + itemPS_CONFIG_SERVICIO_PRODUCTO.GetValue("_id").ToString() + "Numero de PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion actializadas: " + Conteo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion > 0)
                        {
                            Archivo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_CONFIG_SERVICIO_PRODUCTO_elementos_configuracion.Close();
            }

        }

        internal static void Extractor_PS_CONSECUTIVO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_CONSECUTIVO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_CONSECUTIVO = null;

            int Conteo_PS_CONSECUTIVO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_CONSECUTIVO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_CONSECUTIVO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_CONSECUTIVO");
                FilterDefinitionBuilder<BsonDocument> builderPS_CONSECUTIVO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_CONSECUTIVO = builderPS_CONSECUTIVO.Empty;

                if (tipo == "")
                {
                    filterPS_CONSECUTIVO = builderPS_CONSECUTIVO.Or(builderPS_CONSECUTIVO.Eq("Actualizacion_Extractor", "1"), !builderPS_CONSECUTIVO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_CONSECUTIVO = builderPS_CONSECUTIVO.And(builderPS_CONSECUTIVO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_CONSECUTIVO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_CONSECUTIVO = Col_PS.Find(filterPS_CONSECUTIVO).ToList();

                if (consulta_PS_CONSECUTIVO != null && consulta_PS_CONSECUTIVO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_CONSECUTIVO encontrados " + consulta_PS_CONSECUTIVO.Count.ToString());
                        foreach (BsonDocument itemPS_CONSECUTIVO in consulta_PS_CONSECUTIVO)
                        {
                            id_mongo = itemPS_CONSECUTIVO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_CONSECUTIVO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_CONSECUTIVO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_CONSECUTIVO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_CONSECUTIVO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_CONSECUTIVO.GetValue("_id")?.ToString()) ? (itemPS_CONSECUTIVO.GetValue("_id").ToString().Length > 30 ? itemPS_CONSECUTIVO.GetValue("_id").ToString().Substring(0, 29) : itemPS_CONSECUTIVO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONSECUTIVO.Contains("fecha_creacion") && !itemPS_CONSECUTIVO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONSECUTIVO.GetValue("fecha_creacion").ToString()) ? (itemPS_CONSECUTIVO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_CONSECUTIVO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_CONSECUTIVO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONSECUTIVO.Contains("usuario_creacion") && !itemPS_CONSECUTIVO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONSECUTIVO.GetValue("usuario_creacion").ToString()) ? (itemPS_CONSECUTIVO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_CONSECUTIVO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_CONSECUTIVO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONSECUTIVO.Contains("fecha_actualizacion") && !itemPS_CONSECUTIVO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONSECUTIVO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_CONSECUTIVO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_CONSECUTIVO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_CONSECUTIVO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONSECUTIVO.Contains("usuario_modificacion") && !itemPS_CONSECUTIVO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CONSECUTIVO.GetValue("usuario_modificacion").ToString()) ? (itemPS_CONSECUTIVO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_CONSECUTIVO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_CONSECUTIVO.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONSECUTIVO.Contains("entidad") ? !string.IsNullOrEmpty(itemPS_CONSECUTIVO.GetValue("entidad")?.ToString()) ? (itemPS_CONSECUTIVO.GetValue("entidad").ToString().Length > 30 ? itemPS_CONSECUTIVO.GetValue("entidad").ToString().Substring(0, 29) : itemPS_CONSECUTIVO.GetValue("entidad").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONSECUTIVO.Contains("valor") ? !string.IsNullOrEmpty(itemPS_CONSECUTIVO.GetValue("valor")?.ToString()) ? (itemPS_CONSECUTIVO.GetValue("valor").ToString().Length > 30 ? itemPS_CONSECUTIVO.GetValue("valor").ToString().Substring(0, 29) : itemPS_CONSECUTIVO.GetValue("valor").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONSECUTIVO.Contains("es_activo") && !itemPS_CONSECUTIVO.GetValue("es_activo").IsBsonNull ? itemPS_CONSECUTIVO.GetValue("es_activo").ToString().Length > 8 ? itemPS_CONSECUTIVO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_CONSECUTIVO.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CONSECUTIVO.Contains("formato") ? !string.IsNullOrEmpty(itemPS_CONSECUTIVO.GetValue("formato")?.ToString()) ? (itemPS_CONSECUTIVO.GetValue("formato").ToString().Length > 30 ? itemPS_CONSECUTIVO.GetValue("formato").ToString().Substring(0, 29) : itemPS_CONSECUTIVO.GetValue("formato").ToString()) : "" : "");
                                        

                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_CONSECUTIVO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_CONSECUTIVO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONSECUTIVO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_CONSECUTIVO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_CONSECUTIVO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CONSECUTIVO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_CONSECUTIVO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_CONSECUTIVO ACTUALIZADA: " + itemPS_CONSECUTIVO.GetValue("_id").ToString() + "Numero de PS_CONSECUTIVO actializadas: " + Conteo_PS_CONSECUTIVO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_CONSECUTIVO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_CONSECUTIVO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_CONSECUTIVO > 0)
                        {
                            Archivo_PS_CONSECUTIVO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_CONSECUTIVO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_CONSECUTIVO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_CONSECUTIVO.Close();
            }

        }

        internal static void Extractor_PS_CRONOMETRO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_CRONOMETRO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_CRONOMETRO = null;

            int Conteo_PS_CRONOMETRO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_CRONOMETRO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_CRONOMETRO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_CRONOMETRO");
                FilterDefinitionBuilder<BsonDocument> builderPS_CRONOMETRO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_CRONOMETRO = builderPS_CRONOMETRO.Empty;

                if (tipo == "")
                {
                    filterPS_CRONOMETRO = builderPS_CRONOMETRO.Or(builderPS_CRONOMETRO.Eq("Actualizacion_Extractor", "1"), !builderPS_CRONOMETRO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_CRONOMETRO = builderPS_CRONOMETRO.And(builderPS_CRONOMETRO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_CRONOMETRO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_CRONOMETRO = Col_PS.Find(filterPS_CRONOMETRO).ToList();

                if (consulta_PS_CRONOMETRO != null && consulta_PS_CRONOMETRO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_CRONOMETRO encontrados " + consulta_PS_CRONOMETRO.Count.ToString());
                        foreach (BsonDocument itemPS_CRONOMETRO in consulta_PS_CRONOMETRO)
                        {
                            id_mongo = itemPS_CRONOMETRO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_CRONOMETRO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_CRONOMETRO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_CRONOMETRO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_CRONOMETRO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_CRONOMETRO.GetValue("_id")?.ToString()) ? (itemPS_CRONOMETRO.GetValue("_id").ToString().Length > 30 ? itemPS_CRONOMETRO.GetValue("_id").ToString().Substring(0, 29) : itemPS_CRONOMETRO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CRONOMETRO.Contains("fecha_creacion") && !itemPS_CRONOMETRO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CRONOMETRO.GetValue("fecha_creacion").ToString()) ? (itemPS_CRONOMETRO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_CRONOMETRO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_CRONOMETRO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CRONOMETRO.Contains("usuario_creacion") && !itemPS_CRONOMETRO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CRONOMETRO.GetValue("usuario_creacion").ToString()) ? (itemPS_CRONOMETRO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_CRONOMETRO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_CRONOMETRO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CRONOMETRO.Contains("fecha_actualizacion") && !itemPS_CRONOMETRO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CRONOMETRO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_CRONOMETRO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_CRONOMETRO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_CRONOMETRO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CRONOMETRO.Contains("usuario_modificacion") && !itemPS_CRONOMETRO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_CRONOMETRO.GetValue("usuario_modificacion").ToString()) ? (itemPS_CRONOMETRO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_CRONOMETRO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_CRONOMETRO.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CRONOMETRO.Contains("nombre") ? !string.IsNullOrEmpty(itemPS_CRONOMETRO.GetValue("nombre")?.ToString()) ? (itemPS_CRONOMETRO.GetValue("nombre").ToString().Length > 30 ? itemPS_CRONOMETRO.GetValue("nombre").ToString().Substring(0, 29) : itemPS_CRONOMETRO.GetValue("nombre").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CRONOMETRO.Contains("descripcion") && !itemPS_CRONOMETRO.GetValue("descripcion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_CRONOMETRO.GetValue("descripcion").ToString()) ? (itemPS_CRONOMETRO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_CRONOMETRO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_CRONOMETRO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CRONOMETRO.Contains("es_activo") && !itemPS_CRONOMETRO.GetValue("es_activo").IsBsonNull ? itemPS_CRONOMETRO.GetValue("es_activo").ToString().Length > 8 ? itemPS_CRONOMETRO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_CRONOMETRO.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_CRONOMETRO.Contains("color") ? !string.IsNullOrEmpty(itemPS_CRONOMETRO.GetValue("color")?.ToString()) ? (itemPS_CRONOMETRO.GetValue("color").ToString().Length > 30 ? itemPS_CRONOMETRO.GetValue("color").ToString().Substring(0, 29) : itemPS_CRONOMETRO.GetValue("color").ToString()) : "" : "");


                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_CRONOMETRO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_CRONOMETRO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CRONOMETRO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_CRONOMETRO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_CRONOMETRO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_CRONOMETRO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_CRONOMETRO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_CRONOMETRO ACTUALIZADA: " + itemPS_CRONOMETRO.GetValue("_id").ToString() + "Numero de PS_CRONOMETRO actializadas: " + Conteo_PS_CRONOMETRO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_CRONOMETRO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_CRONOMETRO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_CRONOMETRO > 0)
                        {
                            Archivo_PS_CRONOMETRO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_CRONOMETRO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_CRONOMETRO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_CRONOMETRO.Close();
            }

        }

        internal static void Extractor_PS_ESTADO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_ESTADO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_ESTADO = null;

            int Conteo_PS_ESTADO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_ESTADO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_ESTADO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_ESTADO");
                FilterDefinitionBuilder<BsonDocument> builderPS_ESTADO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_ESTADO = builderPS_ESTADO.Empty;

                if (tipo == "")
                {
                    filterPS_ESTADO = builderPS_ESTADO.Or(builderPS_ESTADO.Eq("Actualizacion_Extractor", "1"), !builderPS_ESTADO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_ESTADO = builderPS_ESTADO.And(builderPS_ESTADO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_ESTADO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_ESTADO = Col_PS.Find(filterPS_ESTADO).ToList();

                if (consulta_PS_ESTADO != null && consulta_PS_ESTADO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_ESTADO encontrados " + consulta_PS_ESTADO.Count.ToString());
                        foreach (BsonDocument itemPS_ESTADO in consulta_PS_ESTADO)
                        {
                            id_mongo = itemPS_ESTADO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_ESTADO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_ESTADO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_ESTADO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_ESTADO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_ESTADO.GetValue("_id")?.ToString()) ? (itemPS_ESTADO.GetValue("_id").ToString().Length > 30 ? itemPS_ESTADO.GetValue("_id").ToString().Substring(0, 29) : itemPS_ESTADO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("fecha_creacion") && !itemPS_ESTADO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ESTADO.GetValue("fecha_creacion").ToString()) ? (itemPS_ESTADO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_ESTADO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_ESTADO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("usuario_creacion") && !itemPS_ESTADO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ESTADO.GetValue("usuario_creacion").ToString()) ? (itemPS_ESTADO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_ESTADO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_ESTADO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("fecha_actualizacion") && !itemPS_ESTADO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ESTADO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_ESTADO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_ESTADO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_ESTADO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("usuario_modificacion") && !itemPS_ESTADO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ESTADO.GetValue("usuario_modificacion").ToString()) ? (itemPS_ESTADO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_ESTADO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_ESTADO.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("nombre_estado") ? !string.IsNullOrEmpty(itemPS_ESTADO.GetValue("nombre_estado")?.ToString()) ? (itemPS_ESTADO.GetValue("nombre_estado").ToString().Length > 30 ? itemPS_ESTADO.GetValue("nombre_estado").ToString().Substring(0, 29) : itemPS_ESTADO.GetValue("nombre_estado").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("id_reloj") ? !string.IsNullOrEmpty(itemPS_ESTADO.GetValue("id_reloj")?.ToString()) ? (itemPS_ESTADO.GetValue("id_reloj").ToString().Length > 30 ? itemPS_ESTADO.GetValue("id_reloj").ToString().Substring(0, 29) : itemPS_ESTADO.GetValue("id_reloj").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("es_activo") && !itemPS_ESTADO.GetValue("es_activo").IsBsonNull ? itemPS_ESTADO.GetValue("es_activo").ToString().Length > 8 ? itemPS_ESTADO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_ESTADO.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("es_inicial_tarea") && !itemPS_ESTADO.GetValue("es_inicial_tarea").IsBsonNull ? itemPS_ESTADO.GetValue("es_inicial_tarea").ToString().Length > 8 ? itemPS_ESTADO.GetValue("es_inicial_tarea").ToString().Substring(0, 8) : itemPS_ESTADO.GetValue("es_inicial_tarea").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ESTADO.Contains("es_final_tarea") && !itemPS_ESTADO.GetValue("es_final_tarea").IsBsonNull ? itemPS_ESTADO.GetValue("es_final_tarea").ToString().Length > 8 ? itemPS_ESTADO.GetValue("es_final_tarea").ToString().Substring(0, 8) : itemPS_ESTADO.GetValue("es_final_tarea").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,


                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_ESTADO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_ESTADO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_ESTADO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_ESTADO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_ESTADO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_ESTADO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_ESTADO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_ESTADO ACTUALIZADA: " + itemPS_ESTADO.GetValue("_id").ToString() + "Numero de PS_ESTADO actializadas: " + Conteo_PS_ESTADO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_ESTADO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_ESTADO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_ESTADO > 0)
                        {
                            Archivo_PS_ESTADO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_ESTADO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_ESTADO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_ESTADO.Close();
            }

        }

        internal static void Extractor_PS_FASE(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_FASE");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_FASE = null;

            int Conteo_PS_FASE = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_FASE_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_FASE = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_ESTADO");
                FilterDefinitionBuilder<BsonDocument> builderPS_FASE = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_FASE = builderPS_FASE.Empty;

                if (tipo == "")
                {
                    filterPS_FASE = builderPS_FASE.Or(builderPS_FASE.Eq("Actualizacion_Extractor", "1"), !builderPS_FASE.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_FASE = builderPS_FASE.And(builderPS_FASE.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_FASE.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_FASE = Col_PS.Find(filterPS_FASE).ToList();

                if (consulta_PS_FASE != null && consulta_PS_FASE.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_FASE encontrados " + consulta_PS_FASE.Count.ToString());
                        foreach (BsonDocument itemPS_FASE in consulta_PS_FASE)
                        {
                            id_mongo = itemPS_FASE.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_FASE.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_FASE.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_FASE.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_FASE.Contains("_id") ? !string.IsNullOrEmpty(itemPS_FASE.GetValue("_id")?.ToString()) ? (itemPS_FASE.GetValue("_id").ToString().Length > 30 ? itemPS_FASE.GetValue("_id").ToString().Substring(0, 29) : itemPS_FASE.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FASE.Contains("fecha_creacion") && !itemPS_FASE.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_FASE.GetValue("fecha_creacion").ToString()) ? (itemPS_FASE.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_FASE.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_FASE.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FASE.Contains("usuario_creacion") && !itemPS_FASE.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_FASE.GetValue("usuario_creacion").ToString()) ? (itemPS_FASE.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_FASE.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_FASE.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FASE.Contains("fecha_actualizacion") && !itemPS_FASE.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_FASE.GetValue("fecha_actualizacion").ToString()) ? (itemPS_FASE.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_FASE.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_FASE.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FASE.Contains("usuario_modificacion") && !itemPS_FASE.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_FASE.GetValue("usuario_modificacion").ToString()) ? (itemPS_FASE.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_FASE.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_FASE.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FASE.Contains("fase") ? !string.IsNullOrEmpty(itemPS_FASE.GetValue("fase")?.ToString()) ? (itemPS_FASE.GetValue("fase").ToString().Length > 30 ? itemPS_FASE.GetValue("fase").ToString().Substring(0, 29) : itemPS_FASE.GetValue("fase").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FASE.Contains("es_activo") && !itemPS_FASE.GetValue("es_activo").IsBsonNull ? itemPS_FASE.GetValue("es_activo").ToString().Length > 8 ? itemPS_FASE.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_FASE.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FASE.Contains("es_inicial") && !itemPS_FASE.GetValue("es_inicial").IsBsonNull ? itemPS_FASE.GetValue("es_inicial").ToString().Length > 8 ? itemPS_FASE.GetValue("es_inicial").ToString().Substring(0, 8) : itemPS_FASE.GetValue("es_inicial").ToString() : "") ; //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,


                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_FASE Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_FASE.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FASE.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_FASE++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_FASE.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FASE.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_FASE++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_FASE ACTUALIZADA: " + itemPS_FASE.GetValue("_id").ToString() + "Numero de PS_FASE actializadas: " + Conteo_PS_FASE);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_FASE en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_FASE para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_FASE > 0)
                        {
                            Archivo_PS_FASE.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_FASE_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_FASE entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_FASE.Close();
            }

        }

        internal static void Extractor_PS_FASE_estados(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_FASE_estados");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_FASE_estados = null;

            int Conteo_PS_FASE_estados = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_FASE_estados_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_FASE_estados = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_FASE = db.GetCollection<BsonDocument>("PS_FASE");
                FilterDefinitionBuilder<BsonDocument> builderPS_FASE = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_FASE = builderPS_FASE.Empty;

                if (tipo == "")
                {
                    filterPS_FASE = builderPS_FASE.Or(builderPS_FASE.Eq("estados.Actualizacion_Extractor", "1"), !builderPS_FASE.Exists("estados.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_FASE = builderPS_FASE.And(builderPS_FASE.Gte("estados.Fecha_extraccion", fechaconsulta.Date), builderPS_FASE.Lt("estados.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_FASE = Col_PS_FASE.Find(filterPS_FASE).ToList();

                if (consulta_PS_FASE != null && consulta_PS_FASE.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_FASE_estados encontrados " + consulta_PS_FASE.Count.ToString());
                        foreach (BsonDocument itemPS_FASE in consulta_PS_FASE)
                        {
                            id_mongo = itemPS_FASE.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_FASE_estados = itemPS_FASE.GetElement("estados").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_FASE_estados != null && consulta_PS_FASE_estados.Count() > 0)
                            {
                                foreach (BsonValue itemPS_FASE_estados in consulta_PS_FASE_estados)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemPS_FASE_estados.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemPS_FASE_estados.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemPS_FASE_estados.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_FASE.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_FASE.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_FASE.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_FASE.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_FASE.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_FASE_estados.ToBsonDocument().Contains("texto") && !itemPS_FASE_estados.ToBsonDocument().GetValue("texto").IsBsonNull && !string.IsNullOrEmpty(itemPS_FASE_estados.ToBsonDocument().GetValue("texto").ToString()) ? (itemPS_FASE_estados.ToBsonDocument().GetValue("texto").ToString().Length > 50 ? itemPS_FASE_estados.ToBsonDocument().GetValue("texto").ToString().Substring(0, 50) : itemPS_FASE_estados.ToBsonDocument().GetValue("texto").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_FASE_estados.ToBsonDocument().Contains("es_inicial") && !itemPS_FASE_estados.ToBsonDocument().GetValue("es_inicial").IsBsonNull ? itemPS_FASE_estados.ToBsonDocument().GetValue("es_inicial").ToString().Length > 8 ? itemPS_FASE_estados.ToBsonDocument().GetValue("es_inicial").ToString().Substring(0, 8) : itemPS_FASE_estados.ToBsonDocument().GetValue("es_inicial").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_FASE_estados Id: " + id_mongo + "," + itemPS_FASE_estados.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_FASE_estados.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_FASE.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FASE.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("estados._id", itemPS_FASE_estados.ToBsonDocument().GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("estados.Actualizacion_Extractor", "0")
                                                                                                .Set("estados.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_FASE_estados++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemPS_FASE_estados.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_FASE.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                    Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FASE.GetValue("_id").ToString())),
                                                                    Builders<BsonDocument>.Filter.Eq("estados._id", itemPS_FASE_estados.ToBsonDocument().GetValue("_id").ToString())),
                                                                    Builders<BsonDocument>.Update.Set("estados.Actualizacion_Extractor", "0")
                                                                                                 .Set("estados.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_FASE_estados++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_FASE_estados ACTUALIZADA: " + itemPS_FASE.GetValue("_id").ToString() + "Numero de PS_FASE_estados actializadas: " + Conteo_PS_FASE_estados);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_FASE_estados en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_FASE_estados para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_FASE_estados > 0)
                        {
                            Archivo_PS_FASE_estados.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_FASE_estados_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_FASE_estados entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_FASE_estados.Close();
            }

        }

        internal static void Extractor_PS_FORMATO_SALIDA(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_APROBACION");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_FORMATO_SALIDA = null;

            int Conteo_PS_FORMATO_SALIDA = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_FORMATO_SALIDA_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_FORMATO_SALIDA = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_FORMATO_SALIDA = db.GetCollection<BsonDocument>("PS_FORMATO_SALIDA");
                FilterDefinitionBuilder<BsonDocument> builderPS_FORMATO_SALIDA = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.Empty;

                if (tipo == "")
                {
                    filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.Or(builderPS_FORMATO_SALIDA.Eq("Actualizacion_Extractor", "1"), !builderPS_FORMATO_SALIDA.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);                    
                    filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.And(builderPS_FORMATO_SALIDA.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_FORMATO_SALIDA.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_FORMATO_SALIDA = Col_PS_FORMATO_SALIDA.Find(filterPS_FORMATO_SALIDA).ToList();

                if (consulta_PS_FORMATO_SALIDA != null && consulta_PS_FORMATO_SALIDA.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_FORMATO_SALIDA encontrados " + consulta_PS_FORMATO_SALIDA.Count.ToString());
                        foreach (BsonDocument itemPS_FORMATO_SALIDA in consulta_PS_FORMATO_SALIDA)
                        {
                            id_mongo = itemPS_FORMATO_SALIDA.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_FORMATO_SALIDA.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_FORMATO_SALIDA.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_FORMATO_SALIDA.Contains("_id") ? !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("_id")?.ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("_id").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("_id").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("fecha_creacion") && !itemPS_FORMATO_SALIDA.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("fecha_creacion").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_FORMATO_SALIDA.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("usuario_creacion") && !itemPS_FORMATO_SALIDA.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("usuario_creacion").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_FORMATO_SALIDA.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_FORMATO_SALIDA.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("fecha_actualizacion") && !itemPS_FORMATO_SALIDA.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("fecha_actualizacion").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_FORMATO_SALIDA.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("usuario_modificacion") && !itemPS_FORMATO_SALIDA.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("usuario_modificacion").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_FORMATO_SALIDA.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_FORMATO_SALIDA.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("fecha_entrega") && !itemPS_FORMATO_SALIDA.GetValue("fecha_entrega").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("fecha_entrega").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("fecha_entrega").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("fecha_entrega").ToString().Substring(0, 30) : itemPS_FORMATO_SALIDA.GetValue("fecha_entrega").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("id_bodega") && !itemPS_FORMATO_SALIDA.GetValue("id_bodega").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("id_bodega").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("id_bodega").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("id_bodega").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("id_bodega").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("bodega") && !itemPS_FORMATO_SALIDA.GetValue("bodega").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("bodega").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("bodega").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("bodega").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("bodega").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("area_consumidora") && !itemPS_FORMATO_SALIDA.GetValue("area_consumidora").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("area_consumidora").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("area_consumidora").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("area_consumidora").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("area_consumidora").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("movimiento_contable") && !itemPS_FORMATO_SALIDA.GetValue("movimiento_contable").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("movimiento_contable").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("movimiento_contable").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("movimiento_contable").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("movimiento_contable").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("solicitado") && !itemPS_FORMATO_SALIDA.GetValue("solicitado").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("solicitado").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("solicitado").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("solicitado").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("solicitado").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("empresa_instala") && !itemPS_FORMATO_SALIDA.GetValue("empresa_instala").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("empresa_instala").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("empresa_instala").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("empresa_instala").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("empresa_instala").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("lugar_despacho") && !itemPS_FORMATO_SALIDA.GetValue("lugar_despacho").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("lugar_despacho").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("lugar_despacho").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("lugar_despacho").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("lugar_despacho").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("id_aprovisionamiento") && !itemPS_FORMATO_SALIDA.GetValue("id_aprovisionamiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("id_aprovisionamiento").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("id_aprovisionamiento").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("id_aprovisionamiento").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("id_aprovisionamiento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("nombre_cliente") && !itemPS_FORMATO_SALIDA.GetValue("nombre_cliente").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("nombre_cliente").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("nombre_cliente").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("nombre_cliente").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("nombre_cliente").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("direccion_cliente") && !itemPS_FORMATO_SALIDA.GetValue("direccion_cliente").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("direccion_cliente").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("direccion_cliente").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("direccion_cliente").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("direccion_cliente").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("ciudad_cliente") && !itemPS_FORMATO_SALIDA.GetValue("ciudad_cliente").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("ciudad_cliente").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("ciudad_cliente").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("ciudad_cliente").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("ciudad_cliente").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("telefono_cliente") && !itemPS_FORMATO_SALIDA.GetValue("telefono_cliente").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("telefono_cliente").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("telefono_cliente").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("telefono_cliente").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("telefono_cliente").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("contacto_cliente") && !itemPS_FORMATO_SALIDA.GetValue("contacto_cliente").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("contacto_cliente").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("contacto_cliente").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("contacto_cliente").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("contacto_cliente").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("orden") && !itemPS_FORMATO_SALIDA.GetValue("orden").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("orden").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("orden").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("orden").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("orden").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("consecutivo") && !itemPS_FORMATO_SALIDA.GetValue("consecutivo").IsBsonNull && !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.GetValue("consecutivo").ToString()) ? (itemPS_FORMATO_SALIDA.GetValue("consecutivo").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.GetValue("consecutivo").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.GetValue("consecutivo").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("recibido") && !itemPS_FORMATO_SALIDA.GetValue("recibido").IsBsonNull ? itemPS_FORMATO_SALIDA.GetValue("recibido").ToString().Length > 8 ? itemPS_FORMATO_SALIDA.GetValue("recibido").ToString().Substring(0, 8) : itemPS_FORMATO_SALIDA.GetValue("recibido").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FORMATO_SALIDA.Contains("impresiones") && !itemPS_FORMATO_SALIDA.GetValue("impresiones").IsBsonNull ? itemPS_FORMATO_SALIDA.GetValue("impresiones").ToString().Length > 8 ? itemPS_FORMATO_SALIDA.GetValue("impresiones").ToString().Substring(0, 8) : itemPS_FORMATO_SALIDA.GetValue("impresiones").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_FORMATO_SALIDA Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_FORMATO_SALIDA.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS_FORMATO_SALIDA.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FORMATO_SALIDA.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_FORMATO_SALIDA++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_FORMATO_SALIDA.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS_FORMATO_SALIDA.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FORMATO_SALIDA.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_FORMATO_SALIDA++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_FORMATO_SALIDA ACTUALIZADA: " + itemPS_FORMATO_SALIDA.GetValue("_id").ToString() + "Numero de PS_FORMATO_SALIDA actializadas: " + Conteo_PS_FORMATO_SALIDA);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_FORMATO_SALIDA en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_FORMATO_SALIDA para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_FORMATO_SALIDA > 0)
                        {
                            Archivo_PS_FORMATO_SALIDA.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_FORMATO_SALIDA_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_FORMATO_SALIDA entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_FORMATO_SALIDA.Close();
            }

        }

        internal static void Extractor_PS_FORMATO_SALIDA_usuario_solicita(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_FORMATO_SALIDA_usuario_solicita");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_FORMATO_SALIDA_usuario_solicita = null;

            int Conteo_PS_FORMATO_SALIDA_usuario_solicita = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_FORMATO_SALIDA_usuario_solicita_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_FORMATO_SALIDA_usuario_solicita = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_FORMATO_SALIDA = db.GetCollection<BsonDocument>("PS_FORMATO_SALIDA");
                FilterDefinitionBuilder<BsonDocument> builderPS_FORMATO_SALIDA = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.Empty;

                if (tipo == "")
                {
                    filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.Or(builderPS_FORMATO_SALIDA.Eq("usuario_solicita.Actualizacion_Extractor", "1"), !builderPS_FORMATO_SALIDA.Exists("usuario_solicita.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.And(builderPS_FORMATO_SALIDA.Gte("usuario_solicita.Fecha_extraccion", fechaconsulta.Date), builderPS_FORMATO_SALIDA.Lt("usuario_solicita.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_FORMATO_SALIDA = Col_PS_FORMATO_SALIDA.Find(filterPS_FORMATO_SALIDA).ToList();

                if (consulta_PS_FORMATO_SALIDA != null && consulta_PS_FORMATO_SALIDA.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_FORMATO_SALIDA_usuario_solicita encontrados " + consulta_PS_FORMATO_SALIDA.Count.ToString());
                        foreach (BsonDocument itemPS_FORMATO_SALIDA in consulta_PS_FORMATO_SALIDA)
                        {
                            id_mongo = itemPS_FORMATO_SALIDA.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_FORMATO_SALIDA_usuario_solicita = itemPS_FORMATO_SALIDA.GetElement("usuario_solicita").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_FORMATO_SALIDA_usuario_solicita != null && consulta_PS_FORMATO_SALIDA_usuario_solicita.Count() > 0)
                            {
                                foreach (BsonValue itemPS_usuario_solicita in consulta_PS_FORMATO_SALIDA_usuario_solicita)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemPS_usuario_solicita.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemPS_usuario_solicita.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemPS_usuario_solicita.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_FORMATO_SALIDA.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("rol") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("rol").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("rol").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("rol").ToString().Length > 30 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("rol").ToString().Substring(0, 29) : itemPS_usuario_solicita.ToBsonDocument().GetValue("rol").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("id_usuario") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("id_usuario").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("id_usuario").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("id_usuario").ToString().Length > 30 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("id_usuario").ToString().Substring(0, 29) : itemPS_usuario_solicita.ToBsonDocument().GetValue("id_usuario").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("usuario") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("usuario").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("usuario").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("usuario").ToString().Length > 50 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("usuario").ToString().Substring(0, 50) : itemPS_usuario_solicita.ToBsonDocument().GetValue("usuario").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("nombre") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("nombre").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("nombre").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("nombre").ToString().Length > 30 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("nombre").ToString().Substring(0, 29) : itemPS_usuario_solicita.ToBsonDocument().GetValue("nombre").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("apellido") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("apellido").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("apellido").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("apellido").ToString().Length > 30 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("apellido").ToString().Substring(0, 29) : itemPS_usuario_solicita.ToBsonDocument().GetValue("apellido").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("etb") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("etb").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("etb").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("etb").ToString().Length > 30 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("etb").ToString().Substring(0, 29) : itemPS_usuario_solicita.ToBsonDocument().GetValue("etb").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("empresa") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("empresa").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("empresa").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("empresa").ToString().Length > 30 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("empresa").ToString().Substring(0, 29) : itemPS_usuario_solicita.ToBsonDocument().GetValue("empresa").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("identificacion") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("identificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("identificacion").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("identificacion").ToString().Length > 30 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("identificacion").ToString().Substring(0, 29) : itemPS_usuario_solicita.ToBsonDocument().GetValue("identificacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("correo_electronico") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("correo_electronico").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("correo_electronico").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("correo_electronico").ToString().Length > 30 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("correo_electronico").ToString().Substring(0, 29) : itemPS_usuario_solicita.ToBsonDocument().GetValue("correo_electronico").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_solicita.ToBsonDocument().Contains("firma") && !itemPS_usuario_solicita.ToBsonDocument().GetValue("firma").IsBsonNull ? !string.IsNullOrEmpty(itemPS_usuario_solicita.ToBsonDocument().GetValue("firma").ToString()) ? (itemPS_usuario_solicita.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 660000 ? itemPS_usuario_solicita.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_usuario_solicita.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : ""); // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_FORMATO_SALIDA_usuario_solicita Id: " + id_mongo + "," + itemPS_usuario_solicita.ToBsonDocument().GetValue("id_usuario").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_FORMATO_SALIDA_usuario_solicita.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_FORMATO_SALIDA.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FORMATO_SALIDA.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("usuario_solicita.id_usuario", itemPS_usuario_solicita.ToBsonDocument().GetValue("id_usuario").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("usuario_solicita.Actualizacion_Extractor", "0")
                                                                                                .Set("usuario_solicita.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_FORMATO_SALIDA_usuario_solicita++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemPS_usuario_solicita.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_FORMATO_SALIDA.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FORMATO_SALIDA.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("usuario_solicita.id_usuario", itemPS_usuario_solicita.ToBsonDocument().GetValue("id_usuario").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("usuario_solicita.Actualizacion_Extractor", "0")
                                                                                                .Set("usuario_solicita.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_FORMATO_SALIDA_usuario_solicita++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_FORMATO_SALIDA_usuario_solicita ACTUALIZADA: " + itemPS_FORMATO_SALIDA.GetValue("_id").ToString() + "Numero de PS_FORMATO_SALIDA_usuario_solicita actializadas: " + Conteo_PS_FORMATO_SALIDA_usuario_solicita);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_FORMATO_SALIDA_usuario_solicita en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_FORMATO_SALIDA_usuario_solicita para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_FORMATO_SALIDA_usuario_solicita > 0)
                        {
                            Archivo_PS_FORMATO_SALIDA_usuario_solicita.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_FORMATO_SALIDA_usuario_solicita_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_FORMATO_SALIDA_usuario_solicita entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_FORMATO_SALIDA_usuario_solicita.Close();
            }

        }

        internal static void Extractor_PS_FORMATO_SALIDA_elementos_solicitados(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_FORMATO_SALIDA_elementos_solicitados");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_FORMATO_SALIDA_elementos_solicitados = null;

            int Conteo_PS_FORMATO_SALIDA_elementos_solicitados = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_FORMATO_SALIDA_elementos_solicitados_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_FORMATO_SALIDA_elementos_solicitados = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_FORMATO_SALIDA = db.GetCollection<BsonDocument>("PS_FORMATO_SALIDA");
                FilterDefinitionBuilder<BsonDocument> builderPS_FORMATO_SALIDA = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.Empty;

                if (tipo == "")
                {
                    filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.Or(builderPS_FORMATO_SALIDA.Eq("elementos_solicitados.Actualizacion_Extractor", "1"), !builderPS_FORMATO_SALIDA.Exists("elementos_solicitados.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.And(builderPS_FORMATO_SALIDA.Gte("elementos_solicitados.Fecha_extraccion", fechaconsulta.Date), builderPS_FORMATO_SALIDA.Lt("elementos_solicitados.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_FORMATO_SALIDA = Col_PS_FORMATO_SALIDA.Find(filterPS_FORMATO_SALIDA).ToList();

                if (consulta_PS_FORMATO_SALIDA != null && consulta_PS_FORMATO_SALIDA.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_FORMATO_SALIDA_elementos_solicitados encontrados " + consulta_PS_FORMATO_SALIDA.Count.ToString());
                        foreach (BsonDocument itemPS_FORMATO_SALIDA in consulta_PS_FORMATO_SALIDA)
                        {
                            id_mongo = itemPS_FORMATO_SALIDA.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_FORMATO_SALIDA_elementos_solicitados = itemPS_FORMATO_SALIDA.GetElement("elementos_solicitados").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_FORMATO_SALIDA_elementos_solicitados != null && consulta_PS_FORMATO_SALIDA_elementos_solicitados.Count() > 0)
                            {
                                foreach (BsonValue itemPS_elementos_solicitados in consulta_PS_FORMATO_SALIDA_elementos_solicitados)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemPS_elementos_solicitados.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemPS_elementos_solicitados.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemPS_elementos_solicitados.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_FORMATO_SALIDA.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("item") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("item").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("item").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("item").ToString().Length > 30 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("item").ToString().Substring(0, 29) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("item").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("codigo") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("codigo").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("codigo").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("codigo").ToString().Length > 30 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("codigo").ToString().Substring(0, 29) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("codigo").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("id_inventario") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_inventario").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_inventario").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_inventario").ToString().Length > 30 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_inventario").ToString().Substring(0, 29) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_inventario").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("id_producto") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_producto").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_producto").ToString().Length > 30 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_producto").ToString().Substring(0, 29) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_producto").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("descripcion") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("descripcion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("descripcion").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("unidad") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("unidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("unidad").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("unidad").ToString().Length > 50 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("unidad").ToString().Substring(0, 50) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("unidad").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("cantidad") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("cantidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("cantidad").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("cantidad").ToString().Length > 30 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("cantidad").ToString().Substring(0, 29) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("cantidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("serial") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("serial").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("serial").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("serial").ToString().Length > 30 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("serial").ToString().Substring(0, 29) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("serial").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("ubicacion") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("ubicacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("ubicacion").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("ubicacion").ToString().Length > 30 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("ubicacion").ToString().Substring(0, 29) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("ubicacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,  
                                                "~|" + (itemPS_elementos_solicitados.ToBsonDocument().Contains("reserva") && !itemPS_elementos_solicitados.ToBsonDocument().GetValue("reserva").IsBsonNull && !string.IsNullOrEmpty(itemPS_elementos_solicitados.ToBsonDocument().GetValue("reserva").ToString()) ? (itemPS_elementos_solicitados.ToBsonDocument().GetValue("reserva").ToString().Length > 30 ? itemPS_elementos_solicitados.ToBsonDocument().GetValue("reserva").ToString().Substring(0, 29) : itemPS_elementos_solicitados.ToBsonDocument().GetValue("reserva").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_FORMATO_SALIDA_elementos_solicitados Id: " + id_mongo + "," + itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_inventario").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_FORMATO_SALIDA_elementos_solicitados.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "") //inventario, reserva, producto
                                                        {
                                                            Col_PS_FORMATO_SALIDA.UpdateOne(
                                                                Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FORMATO_SALIDA.GetValue("_id").ToString())),
                                                                        Builders<BsonDocument>.Filter.And(
                                                                        Builders<BsonDocument>.Filter.Eq("elementos_solicitados.id_inventario", MongoDB.Bson.ObjectId.Parse(itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_inventario").ToString())),
                                                                        Builders<BsonDocument>.Filter.Eq("elementos_solicitados.id_producto", MongoDB.Bson.ObjectId.Parse(itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_producto").ToString()))
                                                                                                        ),
                                                                   Builders<BsonDocument>.Filter.Eq("elementos_solicitados.reserva", itemPS_elementos_solicitados.ToBsonDocument().GetValue("reserva").ToString())
                                                                                                ),
                                                                   Builders<BsonDocument>.Update.Set("elementos_solicitados.Actualizacion_Extractor", "0")
                                                                                                .Set("elementos_solicitados.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_FORMATO_SALIDA_elementos_solicitados++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemPS_elementos_solicitados.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_FORMATO_SALIDA.UpdateOne(
                                                                 Builders<BsonDocument>.Filter.And(
                                                                    Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FORMATO_SALIDA.GetValue("_id").ToString())),
                                                                         Builders<BsonDocument>.Filter.And(
                                                                         Builders<BsonDocument>.Filter.Eq("elementos_solicitados.id_inventario", MongoDB.Bson.ObjectId.Parse(itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_inventario").ToString())),
                                                                         Builders<BsonDocument>.Filter.Eq("elementos_solicitados.id_producto", MongoDB.Bson.ObjectId.Parse(itemPS_elementos_solicitados.ToBsonDocument().GetValue("id_producto").ToString()))
                                                                                                         ),
                                                                    Builders<BsonDocument>.Filter.Eq("elementos_solicitados.reserva", itemPS_elementos_solicitados.ToBsonDocument().GetValue("reserva").ToString())
                                                                                                 ),
                                                                    Builders<BsonDocument>.Update.Set("elementos_solicitados.Actualizacion_Extractor", "0")
                                                                                                 .Set("elementos_solicitados.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_FORMATO_SALIDA_elementos_solicitados++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_FORMATO_SALIDA_elementos_solicitados ACTUALIZADA: " + itemPS_FORMATO_SALIDA.GetValue("_id").ToString() + "Numero de PS_FORMATO_SALIDA_elementos_solicitados actializadas: " + Conteo_PS_FORMATO_SALIDA_elementos_solicitados);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_FORMATO_SALIDA_elementos_solicitados en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_FORMATO_SALIDA_elementos_solicitados para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_FORMATO_SALIDA_elementos_solicitados > 0)
                        {
                            Archivo_PS_FORMATO_SALIDA_elementos_solicitados.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_FORMATO_SALIDA_elementos_solicitados_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_FORMATO_SALIDA_elementos_solicitados entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_FORMATO_SALIDA_elementos_solicitados.Close();
            }

        }

        internal static void Extractor_PS_FORMATO_SALIDA_usuario_aprueba(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_FORMATO_SALIDA_usuario_aprueba");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_FORMATO_SALIDA_usuario_aprueba = null;

            int Conteo_PS_FORMATO_SALIDA_usuario_aprueba = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_FORMATO_SALIDA_usuario_aprueba_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_FORMATO_SALIDA_usuario_aprueba = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_FORMATO_SALIDA = db.GetCollection<BsonDocument>("PS_FORMATO_SALIDA");
                FilterDefinitionBuilder<BsonDocument> builderPS_FORMATO_SALIDA = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.Empty;

                if (tipo == "")
                {
                    filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.Or(builderPS_FORMATO_SALIDA.Eq("usuario_aprueba.Actualizacion_Extractor", "1"), !builderPS_FORMATO_SALIDA.Exists("usuario_aprueba.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_FORMATO_SALIDA = builderPS_FORMATO_SALIDA.And(builderPS_FORMATO_SALIDA.Gte("usuario_aprueba.Fecha_extraccion", fechaconsulta.Date), builderPS_FORMATO_SALIDA.Lt("usuario_aprueba.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_FORMATO_SALIDA = Col_PS_FORMATO_SALIDA.Find(filterPS_FORMATO_SALIDA).ToList();

                if (consulta_PS_FORMATO_SALIDA != null && consulta_PS_FORMATO_SALIDA.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_FORMATO_SALIDA_usuario_aprueba encontrados " + consulta_PS_FORMATO_SALIDA.Count.ToString());
                        foreach (BsonDocument itemPS_FORMATO_SALIDA in consulta_PS_FORMATO_SALIDA)
                        {
                            id_mongo = itemPS_FORMATO_SALIDA.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_FORMATO_SALIDA_usuario_aprueba = itemPS_FORMATO_SALIDA.GetElement("usuario_aprueba").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_FORMATO_SALIDA_usuario_aprueba != null && consulta_PS_FORMATO_SALIDA_usuario_aprueba.Count() > 0)
                            {
                                foreach (BsonValue itemPS_usuario_aprueba in consulta_PS_FORMATO_SALIDA_usuario_aprueba)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemPS_usuario_aprueba.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemPS_usuario_aprueba.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemPS_usuario_aprueba.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_FORMATO_SALIDA.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_FORMATO_SALIDA.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("rol") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("rol").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("rol").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("rol").ToString().Length > 30 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("rol").ToString().Substring(0, 29) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("rol").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("id_usuario") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("id_usuario").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("id_usuario").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("id_usuario").ToString().Length > 30 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("id_usuario").ToString().Substring(0, 29) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("id_usuario").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("usuario") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("usuario").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("usuario").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("usuario").ToString().Length > 50 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("usuario").ToString().Substring(0, 50) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("usuario").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("nombre") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("nombre").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("nombre").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("nombre").ToString().Length > 30 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("nombre").ToString().Substring(0, 29) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("nombre").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("apellido") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("apellido").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("apellido").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("apellido").ToString().Length > 30 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("apellido").ToString().Substring(0, 29) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("apellido").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("etb") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("etb").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("etb").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("etb").ToString().Length > 30 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("etb").ToString().Substring(0, 29) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("etb").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("empresa") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("empresa").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("empresa").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("empresa").ToString().Length > 30 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("empresa").ToString().Substring(0, 29) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("empresa").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("identificacion") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("identificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("identificacion").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("identificacion").ToString().Length > 30 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("identificacion").ToString().Substring(0, 29) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("identificacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("correo_electronico") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("correo_electronico").IsBsonNull && !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("correo_electronico").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("correo_electronico").ToString().Length > 30 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("correo_electronico").ToString().Substring(0, 29) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("correo_electronico").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_usuario_aprueba.ToBsonDocument().Contains("firma") && !itemPS_usuario_aprueba.ToBsonDocument().GetValue("firma").IsBsonNull ? !string.IsNullOrEmpty(itemPS_usuario_aprueba.ToBsonDocument().GetValue("firma").ToString()) ? (itemPS_usuario_aprueba.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 660000 ? itemPS_usuario_aprueba.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_usuario_aprueba.ToBsonDocument().GetValue("firma").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : ""); // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_FORMATO_SALIDA_usuario_aprueba Id: " + id_mongo + "," + itemPS_usuario_aprueba.ToBsonDocument().GetValue("id_usuario").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_FORMATO_SALIDA_usuario_aprueba.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_FORMATO_SALIDA.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FORMATO_SALIDA.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("usuario_aprueba.id_usuario", itemPS_usuario_aprueba.ToBsonDocument().GetValue("id_usuario").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("usuario_aprueba.Actualizacion_Extractor", "0")
                                                                                                .Set("usuario_aprueba.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_FORMATO_SALIDA_usuario_aprueba++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemPS_usuario_aprueba.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_FORMATO_SALIDA.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FORMATO_SALIDA.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("usuario_aprueba.id_usuario", itemPS_usuario_aprueba.ToBsonDocument().GetValue("id_usuario").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("usuario_aprueba.Actualizacion_Extractor", "0")
                                                                                                .Set("usuario_aprueba.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_FORMATO_SALIDA_usuario_aprueba++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_FORMATO_SALIDA_usuario_aprueba ACTUALIZADA: " + itemPS_FORMATO_SALIDA.GetValue("_id").ToString() + "Numero de PS_FORMATO_SALIDA_usuario_aprueba actializadas: " + Conteo_PS_FORMATO_SALIDA_usuario_aprueba);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_FORMATO_SALIDA_usuario_aprueba en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_FORMATO_SALIDA_usuario_aprueba para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_FORMATO_SALIDA_usuario_aprueba > 0)
                        {
                            Archivo_PS_FORMATO_SALIDA_usuario_aprueba.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_FORMATO_SALIDA_usuario_aprueba_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_FORMATO_SALIDA_usuario_aprueba entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_FORMATO_SALIDA_usuario_aprueba.Close();
            }

        }

        internal static void Extractor_PS_FUNCIONALIDAD(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_FUNCIONALIDAD");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_FUNCIONALIDAD = null;

            int Conteo_PS_FUNCIONALIDAD = 0;
            int acciones = 0;
            string sTextoDescarga_FUNCIONALIDAD = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_FUNCIONALIDAD_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_FUNCIONALIDAD = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_FUNCIONALIDAD");
                FilterDefinitionBuilder<BsonDocument> builderPS_FUNCIONALIDAD = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_FUNCIONALIDAD = builderPS_FUNCIONALIDAD.Empty;

                if (tipo == "")
                {
                    filterPS_FUNCIONALIDAD = builderPS_FUNCIONALIDAD.Or(builderPS_FUNCIONALIDAD.Eq("Actualizacion_Extractor", "1"), !builderPS_FUNCIONALIDAD.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_FUNCIONALIDAD = builderPS_FUNCIONALIDAD.And(builderPS_FUNCIONALIDAD.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_FUNCIONALIDAD.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_FUNCIONALIDAD = Col_PS.Find(filterPS_FUNCIONALIDAD).ToList();

                if (consulta_PS_FUNCIONALIDAD != null && consulta_PS_FUNCIONALIDAD.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_FUNCIONALIDAD encontrados " + consulta_PS_FUNCIONALIDAD.Count.ToString());
                        foreach (BsonDocument itemPS_FUNCIONALIDAD in consulta_PS_FUNCIONALIDAD)
                        {
                            id_mongo = itemPS_FUNCIONALIDAD.GetValue("_id").ToString();

                            sTextoDescarga_FUNCIONALIDAD = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_FUNCIONALIDAD.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_FUNCIONALIDAD.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_FUNCIONALIDAD.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga_FUNCIONALIDAD =
                                        (itemPS_FUNCIONALIDAD.Contains("_id") ? !string.IsNullOrEmpty(itemPS_FUNCIONALIDAD.GetValue("_id")?.ToString()) ? (itemPS_FUNCIONALIDAD.GetValue("_id").ToString().Length > 30 ? itemPS_FUNCIONALIDAD.GetValue("_id").ToString().Substring(0, 29) : itemPS_FUNCIONALIDAD.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_FUNCIONALIDAD.Contains("nombre_funcionalidad") && !itemPS_FUNCIONALIDAD.GetValue("nombre_funcionalidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_FUNCIONALIDAD.GetValue("nombre_funcionalidad").ToString()) ? (itemPS_FUNCIONALIDAD.GetValue("nombre_funcionalidad").ToString().Length > 50 ? itemPS_FUNCIONALIDAD.GetValue("nombre_funcionalidad").ToString().Substring(0, 50) : itemPS_FUNCIONALIDAD.GetValue("nombre_funcionalidad").ToString()) : ""); // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga_FUNCIONALIDAD = sTextoDescarga_FUNCIONALIDAD.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        if (itemPS_FUNCIONALIDAD.Contains("acciones") && !itemPS_FUNCIONALIDAD.GetValue("acciones").IsBsonNull && itemPS_FUNCIONALIDAD.GetElement("acciones").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                        {
                                            acciones += Extractor_PS_FUNCIONALIDAD_acciones(id_mongo);
                                        }
                                        
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_FUNCIONALIDAD Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga_FUNCIONALIDAD != "")
                                        {
                                            Archivo_PS_FUNCIONALIDAD.WriteLine(sTextoDescarga_FUNCIONALIDAD);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FUNCIONALIDAD.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_FUNCIONALIDAD++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_FUNCIONALIDAD.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_FUNCIONALIDAD.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_FUNCIONALIDAD++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_FUNCIONALIDAD ACTUALIZADA: " + itemPS_FUNCIONALIDAD.GetValue("_id").ToString() + "Numero de PS_FUNCIONALIDAD actializadas: " + Conteo_PS_FUNCIONALIDAD);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_FUNCIONALIDAD en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_FUNCIONALIDAD para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_FUNCIONALIDAD > 0)
                        {
                            Archivo_PS_FUNCIONALIDAD.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_FUNCIONALIDAD_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                if (acciones > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_FUNCIONALIDAD_acciones_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_FUNCIONALIDAD entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_FUNCIONALIDAD.Close();
            }

        }

        internal static int Extractor_PS_FUNCIONALIDAD_acciones(string id_mongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo  PS_FUNCIONALIDAD_acciones");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_FUNCIONALIDAD_acciones = null;

            int Conteo_PS_FUNCIONALIDAD_acciones = 0;
            string sTextoDescarga = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();


            string archivo = path + "PS_FUNCIONALIDAD_acciones_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_FUNCIONALIDAD_acciones = new StreamWriter(archivo, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_FUNCIONALIDAD_acciones = db.GetCollection<BsonDocument>("PS_FUNCIONALIDAD");
                FilterDefinitionBuilder<BsonDocument> builderPS_FUNCIONALIDAD_acciones = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_FUNCIONALIDAD_acciones = builderPS_FUNCIONALIDAD_acciones.Empty;
                filterPS_FUNCIONALIDAD_acciones = builderPS_FUNCIONALIDAD_acciones.And(
                builderPS_FUNCIONALIDAD_acciones.Eq("_id", MongoDB.Bson.ObjectId.Parse(id_mongo)),
                builderPS_FUNCIONALIDAD_acciones.SizeGte("acciones", 1));


                List<BsonDocument> consulta_PS_FUNCIONALIDAD_ac = Col_PS_FUNCIONALIDAD_acciones.Find(filterPS_FUNCIONALIDAD_acciones).ToList();

                if (consulta_PS_FUNCIONALIDAD_ac != null && consulta_PS_FUNCIONALIDAD_ac.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_FUNCIONALIDAD_acciones encontrados " + consulta_PS_FUNCIONALIDAD_ac.Count.ToString());
                        foreach (BsonDocument itemPS_FUNCIONALIDAD in consulta_PS_FUNCIONALIDAD_ac)
                        {
                            id_mongo = itemPS_FUNCIONALIDAD.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_FUNCIONALIDAD_acciones = itemPS_FUNCIONALIDAD.GetElement("acciones").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_FUNCIONALIDAD_acciones != null && consulta_PS_FUNCIONALIDAD_acciones.Count() > 0)
                            {
                                foreach (BsonValue itemPS_FUNCIONALIDAD_acciones in consulta_PS_FUNCIONALIDAD_acciones)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga =
                                            (itemPS_FUNCIONALIDAD.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_FUNCIONALIDAD.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_FUNCIONALIDAD.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_FUNCIONALIDAD.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_FUNCIONALIDAD.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemPS_FUNCIONALIDAD_acciones.ToString());// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");

                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_FUNCIONALIDAD_acciones Id: " + id_mongo + "," + itemPS_FUNCIONALIDAD_acciones.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga != "")
                                            {
                                                Archivo_PS_FUNCIONALIDAD_acciones.WriteLine(sTextoDescarga);
                                                Console.WriteLine(sTextoDescarga);
                                                Conteo_PS_FUNCIONALIDAD_acciones++;
                                            }
                                            //Console.WriteLine("PS_FUNCIONALIDAD_acciones ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_FUNCIONALIDAD_acciones actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_FUNCIONALIDAD_acciones en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_FUNCIONALIDAD_acciones para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_FUNCIONALIDAD_acciones ACTUALIZADA: " + itemPS_FUNCIONALIDAD.GetValue("_id").ToString() + "Numero de PS_FUNCIONALIDAD_acciones actializadas: " + Conteo_PS_FUNCIONALIDAD_acciones);
                            }

                        }

                        if (Conteo_PS_FUNCIONALIDAD_acciones > 0)
                        {
                            Archivo_PS_FUNCIONALIDAD_acciones.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_FUNCIONALIDAD_acciones entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_FUNCIONALIDAD_acciones;
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
                Archivo_PS_FUNCIONALIDAD_acciones.Close();

            }
            return Conteo_PS_FUNCIONALIDAD_acciones;
        } //Se ejecuta con Extractor_PS_FUNCIONALIDAD()

        internal static void Extractor_PS_GRUPO_ASIGNACION(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_GRUPO_ASIGNACION");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_GRUPO_ASIGNACION = null;

            int Conteo_GRUPO_ASIGNACION = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_GRUPO_ASIGNACION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_GRUPO_ASIGNACION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_GRUPO_ASIGNACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_GRUPO_ASIGNACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_GRUPO_ASIGNACION = builderPS_GRUPO_ASIGNACION.Empty;

                if (tipo == "")
                {
                    filterPS_GRUPO_ASIGNACION = builderPS_GRUPO_ASIGNACION.Or(builderPS_GRUPO_ASIGNACION.Eq("Actualizacion_Extractor", "1"), !builderPS_GRUPO_ASIGNACION.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);                   
                    filterPS_GRUPO_ASIGNACION = builderPS_GRUPO_ASIGNACION.And(builderPS_GRUPO_ASIGNACION.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_GRUPO_ASIGNACION.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_GRUPO_ASIGNACION = Col_PS.Find(filterPS_GRUPO_ASIGNACION).ToList();

                if (consulta_PS_GRUPO_ASIGNACION != null && consulta_PS_GRUPO_ASIGNACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_GRUPO_ASIGNACION encontrados " + consulta_PS_GRUPO_ASIGNACION.Count.ToString());
                        foreach (BsonDocument itemPS_GRUPO_ASIGNACION in consulta_PS_GRUPO_ASIGNACION)
                        {
                            id_mongo = itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_GRUPO_ASIGNACION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_GRUPO_ASIGNACION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_GRUPO_ASIGNACION.Contains("_id") ? !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("_id")?.ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString().Length > 30 ? itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString().Substring(0, 29) : itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("fecha_creacion") && !itemPS_GRUPO_ASIGNACION.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("fecha_creacion").ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_GRUPO_ASIGNACION.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_GRUPO_ASIGNACION.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("usuario_creacion") && !itemPS_GRUPO_ASIGNACION.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("usuario_creacion").ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_GRUPO_ASIGNACION.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_GRUPO_ASIGNACION.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("fecha_actualizacion") && !itemPS_GRUPO_ASIGNACION.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("fecha_actualizacion").ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_GRUPO_ASIGNACION.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_GRUPO_ASIGNACION.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("usuario_modificacion") && !itemPS_GRUPO_ASIGNACION.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("usuario_modificacion").ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_GRUPO_ASIGNACION.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_GRUPO_ASIGNACION.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_GRUPO_ASIGNACION.ToBsonDocument().Contains("nombre") && !itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("nombre").IsBsonNull && !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("nombre").ToString()) ? (itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("nombre").ToString().Length > 30 ? itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("nombre").ToString().Substring(0, 29) : itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("nombre").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("es_activo") && !itemPS_GRUPO_ASIGNACION.GetValue("es_activo").IsBsonNull ? itemPS_GRUPO_ASIGNACION.GetValue("es_activo").ToString().Length > 8 ? itemPS_GRUPO_ASIGNACION.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_GRUPO_ASIGNACION.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("direccion") && !itemPS_GRUPO_ASIGNACION.GetValue("direccion").IsBsonNull && !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("direccion").ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("direccion").ToString().Length > 30 ? itemPS_GRUPO_ASIGNACION.GetValue("direccion").ToString().Substring(0, 29) : itemPS_GRUPO_ASIGNACION.GetValue("direccion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("gerencia") && !itemPS_GRUPO_ASIGNACION.GetValue("gerencia").IsBsonNull && !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("gerencia").ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("gerencia").ToString().Length > 30 ? itemPS_GRUPO_ASIGNACION.GetValue("gerencia").ToString().Substring(0, 29) : itemPS_GRUPO_ASIGNACION.GetValue("gerencia").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("tipo_proceso") && !itemPS_GRUPO_ASIGNACION.GetValue("tipo_proceso").IsBsonNull && !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("tipo_proceso").ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("tipo_proceso").ToString().Length > 30 ? itemPS_GRUPO_ASIGNACION.GetValue("tipo_proceso").ToString().Substring(0, 29) : itemPS_GRUPO_ASIGNACION.GetValue("tipo_proceso").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("descripcion") && !itemPS_GRUPO_ASIGNACION.GetValue("descripcion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.GetValue("descripcion").ToString()) ? (itemPS_GRUPO_ASIGNACION.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_GRUPO_ASIGNACION.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_GRUPO_ASIGNACION.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("es_predeterminado_viabilidad") && !itemPS_GRUPO_ASIGNACION.GetValue("es_predeterminado_viabilidad").IsBsonNull ? itemPS_GRUPO_ASIGNACION.GetValue("es_predeterminado_viabilidad").ToString().Length > 8 ? itemPS_GRUPO_ASIGNACION.GetValue("es_predeterminado_viabilidad").ToString().Substring(0, 8) : itemPS_GRUPO_ASIGNACION.GetValue("es_predeterminado_viabilidad").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_GRUPO_ASIGNACION.Contains("es_predeterminado_aprovisionamiento") && !itemPS_GRUPO_ASIGNACION.GetValue("es_predeterminado_aprovisionamiento").IsBsonNull ? itemPS_GRUPO_ASIGNACION.GetValue("es_predeterminado_aprovisionamiento").ToString().Length > 8 ? itemPS_GRUPO_ASIGNACION.GetValue("es_predeterminado_aprovisionamiento").ToString().Substring(0, 8) : itemPS_GRUPO_ASIGNACION.GetValue("es_predeterminado_aprovisionamiento").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                           
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_GRUPO_ASIGNACION Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_GRUPO_ASIGNACION.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_GRUPO_ASIGNACION++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_GRUPO_ASIGNACION.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_GRUPO_ASIGNACION++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_GRUPO_ASIGNACION ACTUALIZADA: " + itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString() + "Numero de PS_GRUPO_ASIGNACION actializadas: " + Conteo_GRUPO_ASIGNACION);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_GRUPO_ASIGNACION en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_GRUPO_ASIGNACION para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_GRUPO_ASIGNACION > 0)
                        {
                            Archivo_PS_GRUPO_ASIGNACION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_GRUPO_ASIGNACION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_GRUPO_ASIGNACION entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_GRUPO_ASIGNACION.Close();
            }

        }

        internal static void Extractor_PS_GRUPO_ASIGNACION_integrantes(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_GRUPO_ASIGNACION_integrantes");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_GRUPO_ASIGNACION_integrantes = null;

            int Conteo_PS_GRUPO_ASIGNACION_integrantes = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_GRUPO_ASIGNACION_integrantes_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_GRUPO_ASIGNACION_integrantes = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_GRUPO_ASIGNACION = db.GetCollection<BsonDocument>("PS_GRUPO_ASIGNACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_GRUPO_ASIGNACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_GRUPO_ASIGNACION = builderPS_GRUPO_ASIGNACION.Empty;

                if (tipo == "")
                {
                    filterPS_GRUPO_ASIGNACION = builderPS_GRUPO_ASIGNACION.Or(builderPS_GRUPO_ASIGNACION.Eq("integrantes.Actualizacion_Extractor", "1"), !builderPS_GRUPO_ASIGNACION.Exists("integrantes.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_GRUPO_ASIGNACION = builderPS_GRUPO_ASIGNACION.And(builderPS_GRUPO_ASIGNACION.Gte("integrantes.Fecha_extraccion", fechaconsulta.Date), builderPS_GRUPO_ASIGNACION.Lt("integrantes.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_GRUPO_ASIGNACION = Col_PS_GRUPO_ASIGNACION.Find(filterPS_GRUPO_ASIGNACION).ToList();

                if (consulta_PS_GRUPO_ASIGNACION != null && consulta_PS_GRUPO_ASIGNACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_GRUPO_ASIGNACION_integrantes encontrados " + consulta_PS_GRUPO_ASIGNACION.Count.ToString());
                        foreach (BsonDocument itemPS_GRUPO_ASIGNACION in consulta_PS_GRUPO_ASIGNACION)
                        {
                            id_mongo = itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_GRUPO_ASIGNACION_integrantes = itemPS_GRUPO_ASIGNACION.GetElement("integrantes").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_GRUPO_ASIGNACION_integrantes != null && consulta_PS_GRUPO_ASIGNACION_integrantes.Count() > 0)
                            {
                                foreach (BsonValue itemPS_integrantes in consulta_PS_GRUPO_ASIGNACION_integrantes)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemPS_integrantes.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemPS_integrantes.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemPS_integrantes.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_GRUPO_ASIGNACION.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_GRUPO_ASIGNACION.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("id_usuario") && !itemPS_integrantes.ToBsonDocument().GetValue("id_usuario").IsBsonNull && !string.IsNullOrEmpty(itemPS_integrantes.ToBsonDocument().GetValue("id_usuario").ToString()) ? (itemPS_integrantes.ToBsonDocument().GetValue("id_usuario").ToString().Length > 30 ? itemPS_integrantes.ToBsonDocument().GetValue("id_usuario").ToString().Substring(0, 29) : itemPS_integrantes.ToBsonDocument().GetValue("id_usuario").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("username") && !itemPS_integrantes.ToBsonDocument().GetValue("username").IsBsonNull && !string.IsNullOrEmpty(itemPS_integrantes.ToBsonDocument().GetValue("username").ToString()) ? (itemPS_integrantes.ToBsonDocument().GetValue("username").ToString().Length > 50 ? itemPS_integrantes.ToBsonDocument().GetValue("username").ToString().Substring(0, 50) : itemPS_integrantes.ToBsonDocument().GetValue("username").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("rol") && !itemPS_integrantes.ToBsonDocument().GetValue("rol").IsBsonNull && !string.IsNullOrEmpty(itemPS_integrantes.ToBsonDocument().GetValue("rol").ToString()) ? (itemPS_integrantes.ToBsonDocument().GetValue("rol").ToString().Length > 30 ? itemPS_integrantes.ToBsonDocument().GetValue("rol").ToString().Substring(0, 29) : itemPS_integrantes.ToBsonDocument().GetValue("rol").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("id_rol") && !itemPS_integrantes.ToBsonDocument().GetValue("id_rol").IsBsonNull && !string.IsNullOrEmpty(itemPS_integrantes.ToBsonDocument().GetValue("id_rol").ToString()) ? (itemPS_integrantes.ToBsonDocument().GetValue("id_rol").ToString().Length > 30 ? itemPS_integrantes.ToBsonDocument().GetValue("id_rol").ToString().Substring(0, 29) : itemPS_integrantes.ToBsonDocument().GetValue("id_rol").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("es_activo") && !itemPS_integrantes.ToBsonDocument().GetValue("es_activo").IsBsonNull ? itemPS_integrantes.ToBsonDocument().GetValue("es_activo").ToString().Length > 8 ? itemPS_integrantes.ToBsonDocument().GetValue("es_activo").ToString().Substring(0, 8) : itemPS_integrantes.ToBsonDocument().GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("es_lider") && !itemPS_integrantes.ToBsonDocument().GetValue("es_lider").IsBsonNull ? itemPS_integrantes.ToBsonDocument().GetValue("es_lider").ToString().Length > 8 ? itemPS_integrantes.ToBsonDocument().GetValue("es_lider").ToString().Substring(0, 8) : itemPS_integrantes.ToBsonDocument().GetValue("es_lider").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("nombres") && !itemPS_integrantes.ToBsonDocument().GetValue("nombres").IsBsonNull && !string.IsNullOrEmpty(itemPS_integrantes.ToBsonDocument().GetValue("nombres").ToString()) ? (itemPS_integrantes.ToBsonDocument().GetValue("nombres").ToString().Length > 30 ? itemPS_integrantes.ToBsonDocument().GetValue("nombres").ToString().Substring(0, 29) : itemPS_integrantes.ToBsonDocument().GetValue("nombres").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("apellidos") && !itemPS_integrantes.ToBsonDocument().GetValue("apellidos").IsBsonNull && !string.IsNullOrEmpty(itemPS_integrantes.ToBsonDocument().GetValue("apellidos").ToString()) ? (itemPS_integrantes.ToBsonDocument().GetValue("apellidos").ToString().Length > 30 ? itemPS_integrantes.ToBsonDocument().GetValue("apellidos").ToString().Substring(0, 29) : itemPS_integrantes.ToBsonDocument().GetValue("apellidos").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemPS_integrantes.ToBsonDocument().Contains("es_principal") && !itemPS_integrantes.ToBsonDocument().GetValue("es_principal").IsBsonNull ? itemPS_integrantes.ToBsonDocument().GetValue("es_principal").ToString().Length > 8 ? itemPS_integrantes.ToBsonDocument().GetValue("es_principal").ToString().Substring(0, 8) : itemPS_integrantes.ToBsonDocument().GetValue("es_principal").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,

                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_GRUPO_ASIGNACION_integrantes Id: " + id_mongo + "," + itemPS_integrantes.ToBsonDocument().GetValue("id_usuario").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_GRUPO_ASIGNACION_integrantes.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_GRUPO_ASIGNACION.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("usuario_solicita.id_usuario", itemPS_integrantes.ToBsonDocument().GetValue("id_usuario").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("usuario_solicita.Actualizacion_Extractor", "0")
                                                                                                .Set("usuario_solicita.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_GRUPO_ASIGNACION_integrantes++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemPS_integrantes.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_GRUPO_ASIGNACION.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("usuario_solicita.id_usuario", itemPS_integrantes.ToBsonDocument().GetValue("id_usuario").ToString())),
                                                                   Builders<BsonDocument>.Update.Set("usuario_solicita.Actualizacion_Extractor", "0")
                                                                                                .Set("usuario_solicita.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_GRUPO_ASIGNACION_integrantes++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_GRUPO_ASIGNACION_integrantes ACTUALIZADA: " + itemPS_GRUPO_ASIGNACION.GetValue("_id").ToString() + "Numero de PS_GRUPO_ASIGNACION_integrantes actializadas: " + Conteo_PS_GRUPO_ASIGNACION_integrantes);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_GRUPO_ASIGNACION_integrantes en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_GRUPO_ASIGNACION_integrantes para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_GRUPO_ASIGNACION_integrantes > 0)
                        {
                            Archivo_PS_GRUPO_ASIGNACION_integrantes.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_GRUPO_ASIGNACION_integrantes_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_GRUPO_ASIGNACION_integrantes entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_GRUPO_ASIGNACION_integrantes.Close();
            }

        }

        internal static void Extractor_PS_HISTORICO_MODIFICACIONES(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_HISTORICO_MODIFICACIONES");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_HISTORICO_MODIFICACIONES = null;

            int Conteo_HISTORICO_MODIFICACIONES = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_HISTORICO_MODIFICACIONES_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_HISTORICO_MODIFICACIONES = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_HISTORICO_MODIFICACIONES");
                FilterDefinitionBuilder<BsonDocument> builderPS_HISTORICO_MODIFICACIONES = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_HISTORICO_MODIFICACIONES = builderPS_HISTORICO_MODIFICACIONES.Empty;

                if (tipo == "")
                {
                    filterPS_HISTORICO_MODIFICACIONES = builderPS_HISTORICO_MODIFICACIONES.Or(builderPS_HISTORICO_MODIFICACIONES.Eq("Actualizacion_Extractor", "1"), !builderPS_HISTORICO_MODIFICACIONES.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_HISTORICO_MODIFICACIONES = builderPS_HISTORICO_MODIFICACIONES.And(builderPS_HISTORICO_MODIFICACIONES.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_HISTORICO_MODIFICACIONES.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_HISTORICO_MODIFICACIONES = Col_PS.Find(filterPS_HISTORICO_MODIFICACIONES).ToList();

                if (consulta_PS_HISTORICO_MODIFICACIONES != null && consulta_PS_HISTORICO_MODIFICACIONES.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_HISTORICO_MODIFICACIONES encontrados " + consulta_PS_HISTORICO_MODIFICACIONES.Count.ToString());
                        foreach (BsonDocument itemPS_HISTORICO_MODIFICACIONES in consulta_PS_HISTORICO_MODIFICACIONES)
                        {
                            id_mongo = itemPS_HISTORICO_MODIFICACIONES.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_HISTORICO_MODIFICACIONES.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_HISTORICO_MODIFICACIONES.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_HISTORICO_MODIFICACIONES.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_HISTORICO_MODIFICACIONES.Contains("_id") ? !string.IsNullOrEmpty(itemPS_HISTORICO_MODIFICACIONES.GetValue("_id")?.ToString()) ? (itemPS_HISTORICO_MODIFICACIONES.GetValue("_id").ToString().Length > 30 ? itemPS_HISTORICO_MODIFICACIONES.GetValue("_id").ToString().Substring(0, 29) : itemPS_HISTORICO_MODIFICACIONES.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_HISTORICO_MODIFICACIONES.Contains("fecha") && !itemPS_HISTORICO_MODIFICACIONES.GetValue("fecha").IsBsonNull && !string.IsNullOrEmpty(itemPS_HISTORICO_MODIFICACIONES.GetValue("fecha").ToString()) ? (itemPS_HISTORICO_MODIFICACIONES.GetValue("fecha").ToString().Length > 30 ? itemPS_HISTORICO_MODIFICACIONES.GetValue("fecha").ToString().Substring(0, 30) : itemPS_HISTORICO_MODIFICACIONES.GetValue("fecha").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_HISTORICO_MODIFICACIONES.Contains("accion") && !itemPS_HISTORICO_MODIFICACIONES.GetValue("accion").IsBsonNull && !string.IsNullOrEmpty(itemPS_HISTORICO_MODIFICACIONES.GetValue("accion").ToString()) ? (itemPS_HISTORICO_MODIFICACIONES.GetValue("accion").ToString().Length > 30 ? itemPS_HISTORICO_MODIFICACIONES.GetValue("accion").ToString().Substring(0, 29) : itemPS_HISTORICO_MODIFICACIONES.GetValue("accion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_HISTORICO_MODIFICACIONES.Contains("id_usuario") && !itemPS_HISTORICO_MODIFICACIONES.GetValue("id_usuario").IsBsonNull && !string.IsNullOrEmpty(itemPS_HISTORICO_MODIFICACIONES.GetValue("id_usuario").ToString()) ? (itemPS_HISTORICO_MODIFICACIONES.GetValue("id_usuario").ToString().Length > 30 ? itemPS_HISTORICO_MODIFICACIONES.GetValue("id_usuario").ToString().Substring(0, 29) : itemPS_HISTORICO_MODIFICACIONES.GetValue("id_usuario").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_HISTORICO_MODIFICACIONES.Contains("usuario") && !itemPS_HISTORICO_MODIFICACIONES.GetValue("usuario").IsBsonNull && !string.IsNullOrEmpty(itemPS_HISTORICO_MODIFICACIONES.GetValue("usuario").ToString()) ? (itemPS_HISTORICO_MODIFICACIONES.GetValue("usuario").ToString().Length > 50 ? itemPS_HISTORICO_MODIFICACIONES.GetValue("usuario").ToString().Substring(0, 50) : itemPS_HISTORICO_MODIFICACIONES.GetValue("usuario").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_HISTORICO_MODIFICACIONES.Contains("comentarios") && !itemPS_HISTORICO_MODIFICACIONES.GetValue("comentarios").IsBsonNull ? !string.IsNullOrEmpty(itemPS_HISTORICO_MODIFICACIONES.GetValue("comentarios").ToString()) ? (itemPS_HISTORICO_MODIFICACIONES.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_HISTORICO_MODIFICACIONES.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_HISTORICO_MODIFICACIONES.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_HISTORICO_MODIFICACIONES.Contains("ip") && !itemPS_HISTORICO_MODIFICACIONES.GetValue("ip").IsBsonNull && !string.IsNullOrEmpty(itemPS_HISTORICO_MODIFICACIONES.GetValue("ip").ToString()) ? (itemPS_HISTORICO_MODIFICACIONES.GetValue("ip").ToString().Length > 50 ? itemPS_HISTORICO_MODIFICACIONES.GetValue("ip").ToString().Substring(0, 50) : itemPS_HISTORICO_MODIFICACIONES.GetValue("ip").ToString()) : "")+ // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_HISTORICO_MODIFICACIONES.Contains("objeto_serializado") && !itemPS_HISTORICO_MODIFICACIONES.GetValue("objeto_serializado").IsBsonNull && !string.IsNullOrEmpty(itemPS_HISTORICO_MODIFICACIONES.GetValue("objeto_serializado").ToString()) ? (itemPS_HISTORICO_MODIFICACIONES.GetValue("objeto_serializado").ToString().Length > 50 ? itemPS_HISTORICO_MODIFICACIONES.GetValue("objeto_serializado").ToString().Substring(0, 50) : itemPS_HISTORICO_MODIFICACIONES.GetValue("objeto_serializado").ToString()) : ""); // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_HISTORICO_MODIFICACIONES Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_HISTORICO_MODIFICACIONES.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_HISTORICO_MODIFICACIONES.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_HISTORICO_MODIFICACIONES++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_HISTORICO_MODIFICACIONES.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_HISTORICO_MODIFICACIONES.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_HISTORICO_MODIFICACIONES++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_HISTORICO_MODIFICACIONES ACTUALIZADA: " + itemPS_HISTORICO_MODIFICACIONES.GetValue("_id").ToString() + "Numero de PS_HISTORICO_MODIFICACIONES actializadas: " + Conteo_HISTORICO_MODIFICACIONES);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_HISTORICO_MODIFICACIONES en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_HISTORICO_MODIFICACIONES para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_HISTORICO_MODIFICACIONES > 0)
                        {
                            Archivo_PS_HISTORICO_MODIFICACIONES.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_HISTORICO_MODIFICACIONES_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_HISTORICO_MODIFICACIONES entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_HISTORICO_MODIFICACIONES.Close();
            }

        }

        internal static void Extractor_PS_INVENTARIO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_INVENTARIO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_INVENTARIO = null;

            int Conteo_PS_INVENTARIO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;
           

            string archivo = path + "PS_INVENTARIO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_INVENTARIO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_INVENTARIO = db.GetCollection<BsonDocument>("PS_INVENTARIO");
                FilterDefinitionBuilder<BsonDocument> builderPS_INVENTARIO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_INVENTARIO = builderPS_INVENTARIO.Empty;

                if (tipo == "")
                {
                    filterPS_INVENTARIO = builderPS_INVENTARIO.Or(builderPS_INVENTARIO.Eq("Actualizacion_Extractor", "1"), !builderPS_INVENTARIO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_INVENTARIO = builderPS_INVENTARIO.And(builderPS_INVENTARIO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_INVENTARIO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_INVENTARIO = Col_PS_INVENTARIO.Find(filterPS_INVENTARIO).ToList();

                if (consulta_PS_INVENTARIO != null && consulta_PS_INVENTARIO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_INVENTARIO encontrados " + consulta_PS_INVENTARIO.Count.ToString());
                        foreach (BsonDocument itemPS_INVENTARIO in consulta_PS_INVENTARIO)
                        {
                            id_mongo = itemPS_INVENTARIO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_INVENTARIO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_INVENTARIO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_INVENTARIO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_INVENTARIO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("_id")?.ToString()) ? (itemPS_INVENTARIO.GetValue("_id").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("_id").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("fecha_creacion") && !itemPS_INVENTARIO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("fecha_creacion").ToString()) ? (itemPS_INVENTARIO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_INVENTARIO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("usuario_creacion") && !itemPS_INVENTARIO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("usuario_creacion").ToString()) ? (itemPS_INVENTARIO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_INVENTARIO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_INVENTARIO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("fecha_actualizacion") && !itemPS_INVENTARIO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_INVENTARIO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_INVENTARIO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("usuario_modificacion") && !itemPS_INVENTARIO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("usuario_modificacion").ToString()) ? (itemPS_INVENTARIO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_INVENTARIO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_INVENTARIO.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("id_producto") && !itemPS_INVENTARIO.GetValue("id_producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("id_producto").ToString()) ? (itemPS_INVENTARIO.GetValue("id_producto").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("id_producto").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("id_producto").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_INVENTARIO.Contains("producto") && !itemPS_INVENTARIO.GetValue("producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("producto").ToString()) ? (itemPS_INVENTARIO.GetValue("producto").ToString().Length > 50 ? itemPS_INVENTARIO.GetValue("producto").ToString().Substring(0, 50) : itemPS_INVENTARIO.GetValue("producto").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("serial") && !itemPS_INVENTARIO.GetValue("serial").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("serial").ToString()) ? (itemPS_INVENTARIO.GetValue("serial").ToString().Length > 50 ? itemPS_INVENTARIO.GetValue("serial").ToString().Substring(0, 50) : itemPS_INVENTARIO.GetValue("serial").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("cantidad") && !itemPS_INVENTARIO.GetValue("cantidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("cantidad").ToString()) ? (itemPS_INVENTARIO.GetValue("cantidad").ToString().Length > 15 ? itemPS_INVENTARIO.GetValue("cantidad").ToString().Substring(0, 15) : itemPS_INVENTARIO.GetValue("cantidad").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("cantidad_reserva") && !itemPS_INVENTARIO.GetValue("cantidad_reserva").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("cantidad_reserva").ToString()) ? (itemPS_INVENTARIO.GetValue("cantidad_reserva").ToString().Length > 15 ? itemPS_INVENTARIO.GetValue("cantidad_reserva").ToString().Substring(0, 15) : itemPS_INVENTARIO.GetValue("cantidad_reserva").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("lote") && !itemPS_INVENTARIO.GetValue("lote").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("lote").ToString()) ? (itemPS_INVENTARIO.GetValue("lote").ToString().Length > 15 ? itemPS_INVENTARIO.GetValue("lote").ToString().Substring(0, 15) : itemPS_INVENTARIO.GetValue("lote").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("estado") ? !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("estado")?.ToString()) ? (itemPS_INVENTARIO.GetValue("estado").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("estado").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("estado").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_INVENTARIO.Contains("id_estado") ? !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("id_estado")?.ToString()) ? (itemPS_INVENTARIO.GetValue("id_estado").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("id_estado").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("id_estado").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_INVENTARIO.Contains("centro_costo") && !itemPS_INVENTARIO.GetValue("centro_costo").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("centro_costo").ToString()) ? (itemPS_INVENTARIO.GetValue("centro_costo").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("centro_costo").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("centro_costo").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_INVENTARIO.Contains("valor_unitario") && !itemPS_INVENTARIO.GetValue("valor_unitario").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("valor_unitario").ToString()) ? (itemPS_INVENTARIO.GetValue("valor_unitario").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("valor_unitario").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("valor_unitario").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_INVENTARIO.Contains("bodega") && !itemPS_INVENTARIO.GetValue("bodega").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("bodega").ToString()) ? (itemPS_INVENTARIO.GetValue("bodega").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("bodega").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("bodega").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_INVENTARIO.Contains("id_bodega") && !itemPS_INVENTARIO.GetValue("id_bodega").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("id_bodega").ToString()) ? (itemPS_INVENTARIO.GetValue("id_bodega").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("id_bodega").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("id_bodega").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_INVENTARIO.Contains("ubicacion") && !itemPS_INVENTARIO.GetValue("ubicacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("ubicacion").ToString()) ? (itemPS_INVENTARIO.GetValue("ubicacion").ToString().Length > 15 ? itemPS_INVENTARIO.GetValue("ubicacion").ToString().Substring(0, 15) : itemPS_INVENTARIO.GetValue("ubicacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_INVENTARIO.Contains("documento") && !itemPS_INVENTARIO.GetValue("documento").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("documento").ToString()) ? (itemPS_INVENTARIO.GetValue("documento").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("documento").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("documento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_INVENTARIO.Contains("posicion") && !itemPS_INVENTARIO.GetValue("posicion").IsBsonNull && !string.IsNullOrEmpty(itemPS_INVENTARIO.GetValue("posicion").ToString()) ? (itemPS_INVENTARIO.GetValue("posicion").ToString().Length > 30 ? itemPS_INVENTARIO.GetValue("posicion").ToString().Substring(0, 29) : itemPS_INVENTARIO.GetValue("posicion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_INVENTARIO.Contains("es_activo") && !itemPS_INVENTARIO.GetValue("es_activo").IsBsonNull ? itemPS_INVENTARIO.GetValue("es_activo").ToString().Length > 8 ? itemPS_INVENTARIO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_INVENTARIO.GetValue("es_activo").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,

                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_INVENTARIO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_INVENTARIO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS_INVENTARIO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_INVENTARIO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_INVENTARIO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_INVENTARIO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS_INVENTARIO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_INVENTARIO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_INVENTARIO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_INVENTARIO ACTUALIZADA: " + itemPS_INVENTARIO.GetValue("_id").ToString() + "Numero de PS_INVENTARIO actializadas: " + Conteo_PS_INVENTARIO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_INVENTARIO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_INVENTARIO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_INVENTARIO > 0)
                        {
                            Archivo_PS_INVENTARIO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_INVENTARIO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");                               
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_INVENTARIO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_INVENTARIO.Close();
            }

        }

        internal static void Extractor_PS_INVENTARIO_accion_inventario(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_INVENTARIO_accion_inventario");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_INVENTARIO_accion_inventario = null;

            int Conteo_PS_INVENTARIO_accion_inventario = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_INVENTARIO_accion_inventario_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_INVENTARIO_accion_inventario = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_INVENTARIO = db.GetCollection<BsonDocument>("PS_INVENTARIO");
                FilterDefinitionBuilder<BsonDocument> builderPS_INVENTARIO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_INVENTARIO = builderPS_INVENTARIO.Empty;

                if (tipo == "")
                {
                    filterPS_INVENTARIO = builderPS_INVENTARIO.Or(builderPS_INVENTARIO.Eq("accion_inventario.Actualizacion_Extractor", "1"), !builderPS_INVENTARIO.Exists("accion_inventario.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_INVENTARIO = builderPS_INVENTARIO.And(builderPS_INVENTARIO.Gte("accion_inventario.Fecha_extraccion", fechaconsulta.Date), builderPS_INVENTARIO.Lt("accion_inventario.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_INVENTARIO = Col_PS_INVENTARIO.Find(filterPS_INVENTARIO).ToList();

                if (consulta_PS_INVENTARIO != null && consulta_PS_INVENTARIO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_INVENTARIO_accion_inventario encontrados " + consulta_PS_INVENTARIO.Count.ToString());
                        foreach (BsonDocument itemPS_INVENTARIO in consulta_PS_INVENTARIO)
                        {
                            id_mongo = itemPS_INVENTARIO.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_INVENTARIO_accion_inventario = itemPS_INVENTARIO.GetElement("accion_inventario").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_INVENTARIO_accion_inventario != null && consulta_PS_INVENTARIO_accion_inventario.Count() > 0)
                            {
                                foreach (BsonValue itemaccion_inventario in consulta_PS_INVENTARIO_accion_inventario)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itemaccion_inventario.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itemaccion_inventario.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itemaccion_inventario.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_INVENTARIO.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_INVENTARIO.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_INVENTARIO.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_INVENTARIO.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_INVENTARIO.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("_id") && !itemaccion_inventario.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(itemaccion_inventario.ToBsonDocument().GetValue("_id").ToString()) ? (itemaccion_inventario.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemaccion_inventario.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemaccion_inventario.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("fecha_creacion") && !itemaccion_inventario.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemaccion_inventario.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (itemaccion_inventario.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? itemaccion_inventario.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : itemaccion_inventario.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("usuario_creacion") && !itemaccion_inventario.ToBsonDocument().GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemaccion_inventario.ToBsonDocument().GetValue("usuario_creacion").ToString()) ? (itemaccion_inventario.ToBsonDocument().GetValue("usuario_creacion").ToString().Length > 50 ? itemaccion_inventario.ToBsonDocument().GetValue("usuario_creacion").ToString().Substring(0, 50) : itemaccion_inventario.ToBsonDocument().GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("fecha_actualizacion") && !itemaccion_inventario.ToBsonDocument().GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemaccion_inventario.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) ? (itemaccion_inventario.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Length > 30 ? itemaccion_inventario.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemaccion_inventario.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("usuario_modificacion") && !itemaccion_inventario.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemaccion_inventario.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (itemaccion_inventario.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? itemaccion_inventario.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemaccion_inventario.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("tipo_accion") && !itemaccion_inventario.ToBsonDocument().GetValue("tipo_accion").IsBsonNull ? itemaccion_inventario.ToBsonDocument().GetValue("tipo_accion").ToString().Length > 30 ? itemaccion_inventario.ToBsonDocument().GetValue("tipo_accion").ToString().Substring(0, 30) : itemaccion_inventario.ToBsonDocument().GetValue("tipo_accion").ToString() : "") +
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("id_tipo_accion") && !itemaccion_inventario.ToBsonDocument().GetValue("id_tipo_accion").IsBsonNull ? itemaccion_inventario.ToBsonDocument().GetValue("id_tipo_accion").ToString().Length > 8 ? itemaccion_inventario.ToBsonDocument().GetValue("id_tipo_accion").ToString().Substring(0, 8) : itemaccion_inventario.ToBsonDocument().GetValue("id_tipo_accion").ToString() : "") +
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("cantidad") && !itemaccion_inventario.ToBsonDocument().GetValue("cantidad").IsBsonNull && !string.IsNullOrEmpty(itemaccion_inventario.ToBsonDocument().GetValue("cantidad").ToString()) ? (itemaccion_inventario.ToBsonDocument().GetValue("cantidad").ToString().Length > 30 ? itemaccion_inventario.ToBsonDocument().GetValue("cantidad").ToString().Substring(0, 29) : itemaccion_inventario.ToBsonDocument().GetValue("cantidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("estado_inicial") && !itemaccion_inventario.ToBsonDocument().GetValue("estado_inicial").IsBsonNull ? itemaccion_inventario.ToBsonDocument().GetValue("estado_inicial").ToString().Length > 30 ? itemaccion_inventario.ToBsonDocument().GetValue("estado_inicial").ToString().Substring(0, 30) : itemaccion_inventario.ToBsonDocument().GetValue("estado_inicial").ToString() : "") +
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("id_estado_inicial") && !itemaccion_inventario.ToBsonDocument().GetValue("id_estado_inicial").IsBsonNull ? itemaccion_inventario.ToBsonDocument().GetValue("id_estado_inicial").ToString().Length > 8 ? itemaccion_inventario.ToBsonDocument().GetValue("id_estado_inicial").ToString().Substring(0, 8) : itemaccion_inventario.ToBsonDocument().GetValue("id_estado_inicial").ToString() : "") +
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("estado_final") && !itemaccion_inventario.ToBsonDocument().GetValue("estado_final").IsBsonNull ? itemaccion_inventario.ToBsonDocument().GetValue("estado_final").ToString().Length > 30 ? itemaccion_inventario.ToBsonDocument().GetValue("estado_final").ToString().Substring(0, 30) : itemaccion_inventario.ToBsonDocument().GetValue("estado_final").ToString() : "") +
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("id_estado_final") && !itemaccion_inventario.ToBsonDocument().GetValue("id_estado_final").IsBsonNull ? itemaccion_inventario.ToBsonDocument().GetValue("id_estado_final").ToString().Length > 8 ? itemaccion_inventario.ToBsonDocument().GetValue("id_estado_final").ToString().Substring(0, 8) : itemaccion_inventario.ToBsonDocument().GetValue("id_estado_final").ToString() : "") +
                                                "~|" + (itemaccion_inventario.ToBsonDocument().Contains("id_aprovisionamiento") && !itemaccion_inventario.ToBsonDocument().GetValue("id_aprovisionamiento").IsBsonNull ? itemaccion_inventario.ToBsonDocument().GetValue("id_aprovisionamiento").ToString().Length > 8 ? itemaccion_inventario.ToBsonDocument().GetValue("id_aprovisionamiento").ToString().Substring(0, 8) : itemaccion_inventario.ToBsonDocument().GetValue("id_aprovisionamiento").ToString() : "");
                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_INVENTARIO_accion_inventario Id: " + id_mongo + "," + itemaccion_inventario.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_INVENTARIO_accion_inventario.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_INVENTARIO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_INVENTARIO.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("accion_inventario._id", MongoDB.Bson.ObjectId.Parse(itemaccion_inventario.ToBsonDocument().GetValue("_id").ToString()))),
                                                                   Builders<BsonDocument>.Update.Set("accion_inventario.Actualizacion_Extractor", "0")
                                                                                                .Set("accion_inventario.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_INVENTARIO_accion_inventario++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itemaccion_inventario.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_INVENTARIO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                  Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_INVENTARIO.GetValue("_id").ToString())),
                                                                  Builders<BsonDocument>.Filter.Eq("accion_inventario._id", MongoDB.Bson.ObjectId.Parse(itemaccion_inventario.ToBsonDocument().GetValue("_id").ToString()))),
                                                                  Builders<BsonDocument>.Update.Set("accion_inventario.Actualizacion_Extractor", "0")
                                                                                               .Set("accion_inventario.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_INVENTARIO_accion_inventario++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_INVENTARIO_accion_inventario ACTUALIZADA: " + itemPS_INVENTARIO.GetValue("_id").ToString() + "Numero de PS_INVENTARIO_accion_inventario actializadas: " + Conteo_PS_INVENTARIO_accion_inventario);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_INVENTARIO_accion_inventario en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_INVENTARIO_accion_inventario para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_INVENTARIO_accion_inventario > 0)
                        {
                            Archivo_PS_INVENTARIO_accion_inventario.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_INVENTARIO_accion_inventario_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_INVENTARIO_accion_inventario entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_INVENTARIO_accion_inventario.Close();
            }

        }

        internal static void Extractor_PS_MOVIMIENTO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_MOVIMIENTO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_MOVIMIENTO = null;

            int Conteo_PS_MOVIMIENTO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;


            string archivo = path + "PS_MOVIMIENTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_MOVIMIENTO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_MOVIMIENTO = db.GetCollection<BsonDocument>("PS_MOVIMIENTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_MOVIMIENTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_MOVIMIENTO = builderPS_MOVIMIENTO.Empty;

                if (tipo == "")
                {
                    filterPS_MOVIMIENTO = builderPS_MOVIMIENTO.Or(builderPS_MOVIMIENTO.Eq("Actualizacion_Extractor", "1"), !builderPS_MOVIMIENTO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_MOVIMIENTO = builderPS_MOVIMIENTO.And(builderPS_MOVIMIENTO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_MOVIMIENTO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_MOVIMIENTO = Col_PS_MOVIMIENTO.Find(filterPS_MOVIMIENTO).ToList();

                if (consulta_PS_MOVIMIENTO != null && consulta_PS_MOVIMIENTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_MOVIMIENTO encontrados " + consulta_PS_MOVIMIENTO.Count.ToString());
                        foreach (BsonDocument itemPS_MOVIMIENTO in consulta_PS_MOVIMIENTO)
                        {
                            id_mongo = itemPS_MOVIMIENTO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_MOVIMIENTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_MOVIMIENTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_MOVIMIENTO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_MOVIMIENTO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("_id")?.ToString()) ? (itemPS_MOVIMIENTO.GetValue("_id").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("_id").ToString().Substring(0, 29) : itemPS_MOVIMIENTO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("fecha_creacion") && !itemPS_MOVIMIENTO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("fecha_creacion").ToString()) ? (itemPS_MOVIMIENTO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_MOVIMIENTO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("usuario_creacion") && !itemPS_MOVIMIENTO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("usuario_creacion").ToString()) ? (itemPS_MOVIMIENTO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_MOVIMIENTO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_MOVIMIENTO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("fecha_actualizacion") && !itemPS_MOVIMIENTO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_MOVIMIENTO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_MOVIMIENTO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("usuario_modificacion") && !itemPS_MOVIMIENTO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("usuario_modificacion").ToString()) ? (itemPS_MOVIMIENTO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_MOVIMIENTO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_MOVIMIENTO.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("id_inventario") && !itemPS_MOVIMIENTO.GetValue("id_inventario").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("id_inventario").ToString()) ? (itemPS_MOVIMIENTO.GetValue("id_inventario").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("id_inventario").ToString().Substring(0, 29) : itemPS_MOVIMIENTO.GetValue("id_inventario").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_MOVIMIENTO.Contains("id_movimiento") && !itemPS_MOVIMIENTO.GetValue("id_movimiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("id_movimiento").ToString()) ? (itemPS_MOVIMIENTO.GetValue("id_movimiento").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("id_movimiento").ToString().Substring(0, 29) : itemPS_MOVIMIENTO.GetValue("id_movimiento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_MOVIMIENTO.Contains("movimiento") && !itemPS_MOVIMIENTO.GetValue("movimiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("movimiento").ToString()) ? (itemPS_MOVIMIENTO.GetValue("movimiento").ToString().Length > 50 ? itemPS_MOVIMIENTO.GetValue("movimiento").ToString().Substring(0, 50) : itemPS_MOVIMIENTO.GetValue("movimiento").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("cantidad") && !itemPS_MOVIMIENTO.GetValue("cantidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("cantidad").ToString()) ? (itemPS_MOVIMIENTO.GetValue("cantidad").ToString().Length > 15 ? itemPS_MOVIMIENTO.GetValue("cantidad").ToString().Substring(0, 15) : itemPS_MOVIMIENTO.GetValue("cantidad").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("id_estado_inicial") ? !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("id_estado_inicial")?.ToString()) ? (itemPS_MOVIMIENTO.GetValue("id_estado_inicial").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("id_estado_inicial").ToString().Substring(0, 29) : itemPS_MOVIMIENTO.GetValue("id_estado_inicial").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_MOVIMIENTO.Contains("estado_inicial") && !itemPS_MOVIMIENTO.GetValue("estado_inicial").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("estado_inicial").ToString()) ? (itemPS_MOVIMIENTO.GetValue("estado_inicial").ToString().Length > 50 ? itemPS_MOVIMIENTO.GetValue("estado_inicial").ToString().Substring(0, 50) : itemPS_MOVIMIENTO.GetValue("estado_inicial").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("id_estado_final") && !itemPS_MOVIMIENTO.GetValue("id_estado_final").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("id_estado_final").ToString()) ? (itemPS_MOVIMIENTO.GetValue("id_estado_final").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("id_estado_final").ToString().Substring(0, 29) : itemPS_MOVIMIENTO.GetValue("id_estado_final").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_MOVIMIENTO.Contains("estado_final") && !itemPS_MOVIMIENTO.GetValue("estado_final").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("estado_final").ToString()) ? (itemPS_MOVIMIENTO.GetValue("estado_final").ToString().Length > 50 ? itemPS_MOVIMIENTO.GetValue("estado_final").ToString().Substring(0, 50) : itemPS_MOVIMIENTO.GetValue("estado_final").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("id_bodega_inicial") && !itemPS_MOVIMIENTO.GetValue("id_bodega_inicial").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("id_bodega_inicial").ToString()) ? (itemPS_MOVIMIENTO.GetValue("id_bodega_inicial").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("id_bodega_inicial").ToString().Substring(0, 29) : itemPS_MOVIMIENTO.GetValue("id_bodega_inicial").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_MOVIMIENTO.Contains("bodega_inicial") && !itemPS_MOVIMIENTO.GetValue("bodega_inicial").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("bodega_inicial").ToString()) ? (itemPS_MOVIMIENTO.GetValue("bodega_inicial").ToString().Length > 50 ? itemPS_MOVIMIENTO.GetValue("bodega_inicial").ToString().Substring(0, 50) : itemPS_MOVIMIENTO.GetValue("bodega_inicial").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("id_bodega_final") && !itemPS_MOVIMIENTO.GetValue("id_bodega_final").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("id_bodega_final").ToString()) ? (itemPS_MOVIMIENTO.GetValue("id_bodega_final").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("id_bodega_final").ToString().Substring(0, 29) : itemPS_MOVIMIENTO.GetValue("id_bodega_final").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_MOVIMIENTO.Contains("bodega_final") && !itemPS_MOVIMIENTO.GetValue("bodega_final").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("bodega_final").ToString()) ? (itemPS_MOVIMIENTO.GetValue("bodega_final").ToString().Length > 50 ? itemPS_MOVIMIENTO.GetValue("bodega_final").ToString().Substring(0, 50) : itemPS_MOVIMIENTO.GetValue("bodega_final").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("observacion") && !itemPS_MOVIMIENTO.GetValue("observacion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("observacion").ToString()) ? (itemPS_MOVIMIENTO.GetValue("observacion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_MOVIMIENTO.GetValue("observacion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_MOVIMIENTO.GetValue("observacion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("fecha_movimiento") && !itemPS_MOVIMIENTO.GetValue("fecha_movimiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_MOVIMIENTO.GetValue("fecha_movimiento").ToString()) ? (itemPS_MOVIMIENTO.GetValue("fecha_movimiento").ToString().Length > 30 ? itemPS_MOVIMIENTO.GetValue("fecha_movimiento").ToString().Substring(0, 30) : itemPS_MOVIMIENTO.GetValue("fecha_movimiento").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_MOVIMIENTO.Contains("es_activo") && !itemPS_MOVIMIENTO.GetValue("es_activo").IsBsonNull ? itemPS_MOVIMIENTO.GetValue("es_activo").ToString().Length > 8 ? itemPS_MOVIMIENTO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_MOVIMIENTO.GetValue("es_activo").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_MOVIMIENTO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_MOVIMIENTO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS_MOVIMIENTO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_MOVIMIENTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_MOVIMIENTO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_MOVIMIENTO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS_MOVIMIENTO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_MOVIMIENTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_MOVIMIENTO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_MOVIMIENTO ACTUALIZADA: " + itemPS_MOVIMIENTO.GetValue("_id").ToString() + "Numero de PS_MOVIMIENTO actializadas: " + Conteo_PS_MOVIMIENTO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_MOVIMIENTO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_MOVIMIENTO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_MOVIMIENTO > 0)
                        {
                            Archivo_PS_MOVIMIENTO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_MOVIMIENTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");                               
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_MOVIMIENTO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_MOVIMIENTO.Close();
            }

        }

        internal static void Extractor_PS_PARAMETRO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_PARAMETRO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_PARAMETRO = null;

            int Conteo_PS_PARAMETRO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;


            string archivo = path + "PS_PARAMETRO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_PARAMETRO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_PARAMETRO = db.GetCollection<BsonDocument>("PS_PARAMETRO");
                FilterDefinitionBuilder<BsonDocument> builderPS_PARAMETRO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_PARAMETRO = builderPS_PARAMETRO.Empty;

                if (tipo == "")
                {
                    filterPS_PARAMETRO = builderPS_PARAMETRO.Or(builderPS_PARAMETRO.Eq("Actualizacion_Extractor", "1"), !builderPS_PARAMETRO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_PARAMETRO = builderPS_PARAMETRO.And(builderPS_PARAMETRO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_PARAMETRO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_PARAMETRO = Col_PS_PARAMETRO.Find(filterPS_PARAMETRO).ToList();

                if (consulta_PS_PARAMETRO != null && consulta_PS_PARAMETRO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_PARAMETRO encontrados " + consulta_PS_PARAMETRO.Count.ToString());
                        foreach (BsonDocument itemPS_PARAMETRO in consulta_PS_PARAMETRO)
                        {
                            id_mongo = itemPS_PARAMETRO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_PARAMETRO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_PARAMETRO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_PARAMETRO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_PARAMETRO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_PARAMETRO.GetValue("_id")?.ToString()) ? (itemPS_PARAMETRO.GetValue("_id").ToString().Length > 30 ? itemPS_PARAMETRO.GetValue("_id").ToString().Substring(0, 29) : itemPS_PARAMETRO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PARAMETRO.Contains("fecha_creacion") && !itemPS_PARAMETRO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PARAMETRO.GetValue("fecha_creacion").ToString()) ? (itemPS_PARAMETRO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_PARAMETRO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_PARAMETRO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PARAMETRO.Contains("usuario_creacion") && !itemPS_PARAMETRO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PARAMETRO.GetValue("usuario_creacion").ToString()) ? (itemPS_PARAMETRO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_PARAMETRO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_PARAMETRO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PARAMETRO.Contains("fecha_actualizacion") && !itemPS_PARAMETRO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PARAMETRO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_PARAMETRO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_PARAMETRO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_PARAMETRO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PARAMETRO.Contains("usuario_modificacion") && !itemPS_PARAMETRO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PARAMETRO.GetValue("usuario_modificacion").ToString()) ? (itemPS_PARAMETRO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_PARAMETRO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_PARAMETRO.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PARAMETRO.Contains("nombre") && !itemPS_PARAMETRO.GetValue("nombre").IsBsonNull && !string.IsNullOrEmpty(itemPS_PARAMETRO.GetValue("nombre").ToString()) ? (itemPS_PARAMETRO.GetValue("nombre").ToString().Length > 50 ? itemPS_PARAMETRO.GetValue("nombre").ToString().Substring(0, 50) : itemPS_PARAMETRO.GetValue("nombre").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PARAMETRO.Contains("valor") && !itemPS_PARAMETRO.GetValue("valor").IsBsonNull && !string.IsNullOrEmpty(itemPS_PARAMETRO.GetValue("valor").ToString()) ? (itemPS_PARAMETRO.GetValue("valor").ToString().Length > 50 ? itemPS_PARAMETRO.GetValue("valor").ToString().Substring(0, 50) : itemPS_PARAMETRO.GetValue("valor").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PARAMETRO.Contains("es_activo") && !itemPS_PARAMETRO.GetValue("es_activo").IsBsonNull ? itemPS_PARAMETRO.GetValue("es_activo").ToString().Length > 8 ? itemPS_PARAMETRO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_PARAMETRO.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PARAMETRO.Contains("modulo") ? !string.IsNullOrEmpty(itemPS_PARAMETRO.GetValue("modulo")?.ToString()) ? (itemPS_PARAMETRO.GetValue("modulo").ToString().Length > 30 ? itemPS_PARAMETRO.GetValue("modulo").ToString().Substring(0, 29) : itemPS_PARAMETRO.GetValue("modulo").ToString()) : "" : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC, 
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");

                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_PARAMETRO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_PARAMETRO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS_PARAMETRO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_PARAMETRO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_PARAMETRO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_PARAMETRO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS_PARAMETRO.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_PARAMETRO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_PARAMETRO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_PARAMETRO ACTUALIZADA: " + itemPS_PARAMETRO.GetValue("_id").ToString() + "Numero de PS_PARAMETRO actializadas: " + Conteo_PS_PARAMETRO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_PARAMETRO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_PARAMETRO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_PARAMETRO > 0)
                        {
                            Archivo_PS_PARAMETRO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_PARAMETRO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");                               
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_PARAMETRO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_PARAMETRO.Close();
            }

        }

        internal static void Extractor_PS_PLANTILLA_COMUNICACION(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_PLANTILLA_COMUNICACION");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_PLANTILLA_COMUNICACION = null;

            int Conteo_PS_PLANTILLA_COMUNICACION = 0;            
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_PLANTILLA_COMUNICACION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_PLANTILLA_COMUNICACION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_PLANTILLA_COMUNICACION");
                FilterDefinitionBuilder<BsonDocument> builderPS_PLANTILLA_COMUNICACION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_PLANTILLA_COMUNICACION = builderPS_PLANTILLA_COMUNICACION.Empty;

                if (tipo == "")
                {
                    filterPS_PLANTILLA_COMUNICACION = builderPS_PLANTILLA_COMUNICACION.Or(builderPS_PLANTILLA_COMUNICACION.Eq("Actualizacion_Extractor", "1"), !builderPS_PLANTILLA_COMUNICACION.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_PLANTILLA_COMUNICACION = builderPS_PLANTILLA_COMUNICACION.And(builderPS_PLANTILLA_COMUNICACION.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_PLANTILLA_COMUNICACION.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_PLANTILLA_COMUNICACION = Col_PS.Find(filterPS_PLANTILLA_COMUNICACION).ToList();

                if (consulta_PS_PLANTILLA_COMUNICACION != null && consulta_PS_PLANTILLA_COMUNICACION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_PLANTILLA_COMUNICACION encontrados " + consulta_PS_PLANTILLA_COMUNICACION.Count.ToString());
                        foreach (BsonDocument itemPS_PLANTILLA_COMUNICACION in consulta_PS_PLANTILLA_COMUNICACION)
                        {
                            id_mongo = itemPS_PLANTILLA_COMUNICACION.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_PLANTILLA_COMUNICACION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_PLANTILLA_COMUNICACION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_PLANTILLA_COMUNICACION.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_PLANTILLA_COMUNICACION.Contains("_id") ? !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("_id")?.ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("_id").ToString().Length > 30 ? itemPS_PLANTILLA_COMUNICACION.GetValue("_id").ToString().Substring(0, 29) : itemPS_PLANTILLA_COMUNICACION.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("fecha_creacion") && !itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_creacion").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("usuario_creacion") && !itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_creacion").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("fecha_actualizacion") && !itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_actualizacion").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_PLANTILLA_COMUNICACION.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("usuario_modificacion") && !itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_modificacion").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_PLANTILLA_COMUNICACION.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("Plantilla") && !itemPS_PLANTILLA_COMUNICACION.GetValue("Plantilla").IsBsonNull && !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("Plantilla").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("Plantilla").ToString().Length > 50 ? itemPS_PLANTILLA_COMUNICACION.GetValue("Plantilla").ToString().Substring(0, 50) : itemPS_PLANTILLA_COMUNICACION.GetValue("Plantilla").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("observaciones") && !itemPS_PLANTILLA_COMUNICACION.GetValue("observaciones").IsBsonNull ? !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("observaciones").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_PLANTILLA_COMUNICACION.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_PLANTILLA_COMUNICACION.GetValue("observaciones").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("htmlPlantilla") ? !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("htmlPlantilla")?.ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("htmlPlantilla").ToString().Length > 30 ? itemPS_PLANTILLA_COMUNICACION.GetValue("htmlPlantilla").ToString().Substring(0, 29) : itemPS_PLANTILLA_COMUNICACION.GetValue("htmlPlantilla").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("es_activo") && !itemPS_PLANTILLA_COMUNICACION.GetValue("es_activo").IsBsonNull ? itemPS_PLANTILLA_COMUNICACION.GetValue("es_activo").ToString().Length > 8 ? itemPS_PLANTILLA_COMUNICACION.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_PLANTILLA_COMUNICACION.GetValue("es_activo").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("id_tipo_plantilla") ? !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("id_tipo_plantilla")?.ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("id_tipo_plantilla").ToString().Length > 30 ? itemPS_PLANTILLA_COMUNICACION.GetValue("id_tipo_plantilla").ToString().Substring(0, 29) : itemPS_PLANTILLA_COMUNICACION.GetValue("id_tipo_plantilla").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("tipo_plantilla") && !itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_plantilla").IsBsonNull && !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_plantilla").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_plantilla").ToString().Length > 50 ? itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_plantilla").ToString().Substring(0, 50) : itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_plantilla").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("id_producto") ? !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("id_producto")?.ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("id_producto").ToString().Length > 30 ? itemPS_PLANTILLA_COMUNICACION.GetValue("id_producto").ToString().Substring(0, 29) : itemPS_PLANTILLA_COMUNICACION.GetValue("id_producto").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("producto") && !itemPS_PLANTILLA_COMUNICACION.GetValue("producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("producto").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("producto").ToString().Length > 50 ? itemPS_PLANTILLA_COMUNICACION.GetValue("producto").ToString().Substring(0, 50) : itemPS_PLANTILLA_COMUNICACION.GetValue("producto").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("id_tipo_operacion") ? !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("id_tipo_operacion")?.ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("id_tipo_operacion").ToString().Length > 30 ? itemPS_PLANTILLA_COMUNICACION.GetValue("id_tipo_operacion").ToString().Substring(0, 29) : itemPS_PLANTILLA_COMUNICACION.GetValue("id_tipo_operacion").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_PLANTILLA_COMUNICACION.Contains("tipo_operacion") && !itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_operacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_operacion").ToString()) ? (itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_operacion").ToString().Length > 50 ? itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_operacion").ToString().Substring(0, 50) : itemPS_PLANTILLA_COMUNICACION.GetValue("tipo_operacion").ToString()) : ""); // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_PLANTILLA_COMUNICACION Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_PLANTILLA_COMUNICACION.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_PLANTILLA_COMUNICACION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_PLANTILLA_COMUNICACION++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_PLANTILLA_COMUNICACION.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_PLANTILLA_COMUNICACION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_PLANTILLA_COMUNICACION++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_PLANTILLA_COMUNICACION ACTUALIZADA: " + itemPS_PLANTILLA_COMUNICACION.GetValue("_id").ToString() + "Numero de PS_PLANTILLA_COMUNICACION actializadas: " + Conteo_PS_PLANTILLA_COMUNICACION);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_PLANTILLA_COMUNICACION en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_PLANTILLA_COMUNICACION para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_PLANTILLA_COMUNICACION > 0)
                        {
                            Archivo_PS_PLANTILLA_COMUNICACION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_PLANTILLA_COMUNICACION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_PLANTILLA_COMUNICACION entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_PLANTILLA_COMUNICACION.Close();
            }

        }

        internal static void Extractor_PS_PRODUCTO(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_PRODUCTO");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_PRODUCTO = null;

            int Conteo_PS_PRODUCTO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_PRODUCTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_PRODUCTO = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_PRODUCTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_PRODUCTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_PRODUCTO = builderPS_PRODUCTO.Empty;

                if (tipo == "")
                {
                    filterPS_PRODUCTO = builderPS_PRODUCTO.Or(builderPS_PRODUCTO.Eq("Actualizacion_Extractor", "1"), !builderPS_PRODUCTO.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_PRODUCTO = builderPS_PRODUCTO.And(builderPS_PRODUCTO.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_PRODUCTO.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_PRODUCTO = Col_PS.Find(filterPS_PRODUCTO).ToList();

                if (consulta_PS_PRODUCTO != null && consulta_PS_PRODUCTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_PRODUCTO encontrados " + consulta_PS_PRODUCTO.Count.ToString());
                        foreach (BsonDocument itemPS_PRODUCTO in consulta_PS_PRODUCTO)
                        {
                            id_mongo = itemPS_PRODUCTO.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_PRODUCTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_PRODUCTO.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_PRODUCTO.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_PRODUCTO.Contains("_id") ? !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("_id")?.ToString()) ? (itemPS_PRODUCTO.GetValue("_id").ToString().Length > 30 ? itemPS_PRODUCTO.GetValue("_id").ToString().Substring(0, 29) : itemPS_PRODUCTO.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("fecha_creacion") && !itemPS_PRODUCTO.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("fecha_creacion").ToString()) ? (itemPS_PRODUCTO.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_PRODUCTO.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_PRODUCTO.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("usuario_creacion") && !itemPS_PRODUCTO.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("usuario_creacion").ToString()) ? (itemPS_PRODUCTO.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("fecha_actualizacion") && !itemPS_PRODUCTO.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("fecha_actualizacion").ToString()) ? (itemPS_PRODUCTO.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_PRODUCTO.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_PRODUCTO.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("usuario_modificacion") && !itemPS_PRODUCTO.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("usuario_modificacion").ToString()) ? (itemPS_PRODUCTO.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("descripcion") && !itemPS_PRODUCTO.GetValue("descripcion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("descripcion").ToString()) ? (itemPS_PRODUCTO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_PRODUCTO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_PRODUCTO.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("codigo") && !itemPS_PRODUCTO.GetValue("codigo").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("codigo").ToString()) ? (itemPS_PRODUCTO.GetValue("codigo").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("codigo").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("codigo").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("fabricante") && !itemPS_PRODUCTO.GetValue("fabricante").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("fabricante").ToString()) ? (itemPS_PRODUCTO.GetValue("fabricante").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("fabricante").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("fabricante").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("modelo") && !itemPS_PRODUCTO.GetValue("modelo").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("modelo").ToString()) ? (itemPS_PRODUCTO.GetValue("modelo").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("modelo").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("modelo").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("id_ramo") ? !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("id_ramo")?.ToString()) ? (itemPS_PRODUCTO.GetValue("id_ramo").ToString().Length > 30 ? itemPS_PRODUCTO.GetValue("id_ramo").ToString().Substring(0, 29) : itemPS_PRODUCTO.GetValue("id_ramo").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_PRODUCTO.Contains("ramo") && !itemPS_PRODUCTO.GetValue("ramo").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("ramo").ToString()) ? (itemPS_PRODUCTO.GetValue("ramo").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("ramo").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("ramo").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("id_tipo_material") ? !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("id_tipo_material")?.ToString()) ? (itemPS_PRODUCTO.GetValue("id_tipo_material").ToString().Length > 30 ? itemPS_PRODUCTO.GetValue("id_tipo_material").ToString().Substring(0, 29) : itemPS_PRODUCTO.GetValue("id_tipo_material").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_PRODUCTO.Contains("tipo_material") && !itemPS_PRODUCTO.GetValue("tipo_material").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("tipo_material").ToString()) ? (itemPS_PRODUCTO.GetValue("tipo_material").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("tipo_material").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("tipo_material").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("id_unidad_medida") ? !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("id_unidad_medida")?.ToString()) ? (itemPS_PRODUCTO.GetValue("id_unidad_medida").ToString().Length > 30 ? itemPS_PRODUCTO.GetValue("id_unidad_medida").ToString().Substring(0, 29) : itemPS_PRODUCTO.GetValue("id_unidad_medida").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_PRODUCTO.Contains("unidad_medida") && !itemPS_PRODUCTO.GetValue("unidad_medida").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("unidad_medida").ToString()) ? (itemPS_PRODUCTO.GetValue("unidad_medida").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("unidad_medida").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("unidad_medida").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("id_agrupador") ? !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("id_agrupador")?.ToString()) ? (itemPS_PRODUCTO.GetValue("id_agrupador").ToString().Length > 30 ? itemPS_PRODUCTO.GetValue("id_agrupador").ToString().Substring(0, 29) : itemPS_PRODUCTO.GetValue("id_agrupador").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_PRODUCTO.Contains("agrupador") && !itemPS_PRODUCTO.GetValue("agrupador").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("agrupador").ToString()) ? (itemPS_PRODUCTO.GetValue("agrupador").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("agrupador").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("agrupador").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("comentarios") && !itemPS_PRODUCTO.GetValue("comentarios").IsBsonNull ? !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("comentarios").ToString()) ? (itemPS_PRODUCTO.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_PRODUCTO.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_PRODUCTO.GetValue("comentarios").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("es_serializable") && !itemPS_PRODUCTO.GetValue("es_serializable").IsBsonNull ? itemPS_PRODUCTO.GetValue("es_serializable").ToString().Length > 8 ? itemPS_PRODUCTO.GetValue("es_serializable").ToString().Substring(0, 8) : itemPS_PRODUCTO.GetValue("es_serializable").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("existencia_minima") && !itemPS_PRODUCTO.GetValue("existencia_minima").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("existencia_minima").ToString()) ? (itemPS_PRODUCTO.GetValue("existencia_minima").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("existencia_minima").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("existencia_minima").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("existencia_maxima") && !itemPS_PRODUCTO.GetValue("existencia_maxima").IsBsonNull && !string.IsNullOrEmpty(itemPS_PRODUCTO.GetValue("existencia_maxima").ToString()) ? (itemPS_PRODUCTO.GetValue("existencia_maxima").ToString().Length > 50 ? itemPS_PRODUCTO.GetValue("existencia_maxima").ToString().Substring(0, 50) : itemPS_PRODUCTO.GetValue("existencia_maxima").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("es_fisico") && !itemPS_PRODUCTO.GetValue("es_fisico").IsBsonNull ? itemPS_PRODUCTO.GetValue("es_fisico").ToString().Length > 8 ? itemPS_PRODUCTO.GetValue("es_fisico").ToString().Substring(0, 8) : itemPS_PRODUCTO.GetValue("es_fisico").ToString() : "") + //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_PRODUCTO.Contains("es_activo") && !itemPS_PRODUCTO.GetValue("es_activo").IsBsonNull ? itemPS_PRODUCTO.GetValue("es_activo").ToString().Length > 8 ? itemPS_PRODUCTO.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_PRODUCTO.GetValue("es_activo").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_PRODUCTO Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_PRODUCTO.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_PRODUCTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_PRODUCTO++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_PRODUCTO.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_PRODUCTO.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_PRODUCTO++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_PRODUCTO ACTUALIZADA: " + itemPS_PRODUCTO.GetValue("_id").ToString() + "Numero de PS_PRODUCTO actializadas: " + Conteo_PS_PRODUCTO);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_PRODUCTO en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_PRODUCTO para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_PRODUCTO > 0)
                        {
                            Archivo_PS_PRODUCTO.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_PRODUCTO_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_PRODUCTO entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_PRODUCTO.Close();
            }

        }

        internal static void Extractor_PS_PRODUCTO_atributos_producto(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_PRODUCTO_atributos_producto");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_PRODUCTO_atributos = null;

            int Conteo_PS_PRODUCTO = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;

            string archivo = path + "PS_PRODUCTO_atributos_producto_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_PRODUCTO_atributos = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_PRODUCTO = db.GetCollection<BsonDocument>("PS_PRODUCTO");
                FilterDefinitionBuilder<BsonDocument> builderPS_PRODUCTO = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_PRODUCTO = builderPS_PRODUCTO.Empty;

                if (tipo == "")
                {
                    filterPS_PRODUCTO = builderPS_PRODUCTO.Or(builderPS_PRODUCTO.Eq("atributos_producto.Actualizacion_Extractor", "1"), !builderPS_PRODUCTO.Exists("atributos_producto.Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    filterPS_PRODUCTO = builderPS_PRODUCTO.And(builderPS_PRODUCTO.Gte("atributos_producto.Fecha_extraccion", fechaconsulta.Date), builderPS_PRODUCTO.Lt("atributos_producto.Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_PRODUCTO = Col_PS_PRODUCTO.Find(filterPS_PRODUCTO).ToList();

                if (consulta_PS_PRODUCTO != null && consulta_PS_PRODUCTO.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_PRODUCTO_atributos_producto encontrados " + consulta_PS_PRODUCTO.Count.ToString());
                        foreach (BsonDocument itemPS_PRODUCTO in consulta_PS_PRODUCTO)
                        {
                            id_mongo = itemPS_PRODUCTO.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_PRODUCTO_atributos_producto = itemPS_PRODUCTO.GetElement("atributos_producto").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_PRODUCTO_atributos_producto != null && consulta_PS_PRODUCTO_atributos_producto.Count() > 0)
                            {
                                foreach (BsonValue itematributos_producto in consulta_PS_PRODUCTO_atributos_producto)
                                {
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(id_mongo)
                                            && (!itematributos_producto.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || itematributos_producto.ToBsonDocument().Contains("Actualizacion_Extractor")
                                            || (itematributos_producto.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                            )
                                        {
                                            try
                                            {
                                                sTextoDescarga =
                                                (itemPS_PRODUCTO.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_PRODUCTO.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_PRODUCTO.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_PRODUCTO.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_PRODUCTO.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (itematributos_producto.ToBsonDocument().Contains("_id") && !itematributos_producto.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(itematributos_producto.ToBsonDocument().GetValue("_id").ToString()) ? (itematributos_producto.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itematributos_producto.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itematributos_producto.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itematributos_producto.ToBsonDocument().Contains("nombre") && !itematributos_producto.ToBsonDocument().GetValue("nombre").IsBsonNull && !string.IsNullOrEmpty(itematributos_producto.ToBsonDocument().GetValue("nombre").ToString()) ? (itematributos_producto.ToBsonDocument().GetValue("nombre").ToString().Length > 30 ? itematributos_producto.ToBsonDocument().GetValue("nombre").ToString().Substring(0, 29) : itematributos_producto.ToBsonDocument().GetValue("nombre").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itematributos_producto.ToBsonDocument().Contains("atributo_sap") && !itematributos_producto.ToBsonDocument().GetValue("atributo_sap").IsBsonNull && !string.IsNullOrEmpty(itematributos_producto.ToBsonDocument().GetValue("atributo_sap").ToString()) ? (itematributos_producto.ToBsonDocument().GetValue("atributo_sap").ToString().Length > 30 ? itematributos_producto.ToBsonDocument().GetValue("atributo_sap").ToString().Substring(0, 29) : itematributos_producto.ToBsonDocument().GetValue("atributo_sap").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itematributos_producto.ToBsonDocument().Contains("descripcion") && !itematributos_producto.ToBsonDocument().GetValue("descripcion").IsBsonNull && !string.IsNullOrEmpty(itematributos_producto.ToBsonDocument().GetValue("descripcion").ToString()) ? (itematributos_producto.ToBsonDocument().GetValue("descripcion").ToString().Length > 30 ? itematributos_producto.ToBsonDocument().GetValue("descripcion").ToString().Substring(0, 29) : itematributos_producto.ToBsonDocument().GetValue("descripcion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itematributos_producto.ToBsonDocument().Contains("tipo_dato") && !itematributos_producto.ToBsonDocument().GetValue("tipo_dato").IsBsonNull && !string.IsNullOrEmpty(itematributos_producto.ToBsonDocument().GetValue("tipo_dato").ToString()) ? (itematributos_producto.ToBsonDocument().GetValue("tipo_dato").ToString().Length > 30 ? itematributos_producto.ToBsonDocument().GetValue("tipo_dato").ToString().Substring(0, 29) : itematributos_producto.ToBsonDocument().GetValue("tipo_dato").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itematributos_producto.ToBsonDocument().Contains("valor") && !itematributos_producto.ToBsonDocument().GetValue("valor").IsBsonNull && !string.IsNullOrEmpty(itematributos_producto.ToBsonDocument().GetValue("valor").ToString()) ? (itematributos_producto.ToBsonDocument().GetValue("valor").ToString().Length > 30 ? itematributos_producto.ToBsonDocument().GetValue("valor").ToString().Substring(0, 29) : itematributos_producto.ToBsonDocument().GetValue("valor").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (itematributos_producto.ToBsonDocument().Contains("es_activo") && !itematributos_producto.ToBsonDocument().GetValue("es_activo").IsBsonNull ? itematributos_producto.ToBsonDocument().GetValue("es_activo").ToString().Length > 8 ? itematributos_producto.ToBsonDocument().GetValue("es_activo").ToString().Substring(0, 8) : itematributos_producto.ToBsonDocument().GetValue("es_activo").ToString() : "");
                                                sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_PRODUCTO_atributos_producto Id: " + id_mongo + "," + itematributos_producto.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                                continue;
                                            }
                                            // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                            try
                                            {
                                                if (sTextoDescarga != "")
                                                {
                                                    Archivo_PS_PRODUCTO_atributos.WriteLine(sTextoDescarga);
                                                    if (pruebas == false)
                                                    {
                                                        if (tipo == "")
                                                        {
                                                            Col_PS_PRODUCTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                   Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_PRODUCTO.GetValue("_id").ToString())),
                                                                   Builders<BsonDocument>.Filter.Eq("atributos_producto._id", MongoDB.Bson.ObjectId.Parse(itematributos_producto.ToBsonDocument().GetValue("_id").ToString()))),
                                                                   Builders<BsonDocument>.Update.Set("atributos_producto.Actualizacion_Extractor", "0")
                                                                                                .Set("atributos_producto.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                            Conteo_PS_PRODUCTO++;
                                                        }
                                                        else if (tipo != "")
                                                        {
                                                            if ((itematributos_producto.ToBsonDocument().GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                            {
                                                                Col_PS_PRODUCTO.UpdateOne(Builders<BsonDocument>.Filter.And(
                                                                  Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_PRODUCTO.GetValue("_id").ToString())),
                                                                  Builders<BsonDocument>.Filter.Eq("atributos_producto._id", MongoDB.Bson.ObjectId.Parse(itematributos_producto.ToBsonDocument().GetValue("_id").ToString()))),
                                                                  Builders<BsonDocument>.Update.Set("atributos_producto.Actualizacion_Extractor", "0")
                                                                                               .Set("atributos_producto.Fecha_extraccion", fechatemp.ToLocalTime()));
                                                                Conteo_PS_PRODUCTO++;
                                                            }

                                                        }

                                                    }
                                                }
                                                Console.WriteLine("PS_PRODUCTO_atributos_producto ACTUALIZADA: " + itemPS_PRODUCTO.GetValue("_id").ToString() + "Numero de PS_PRODUCTO_atributos_producto actializadas: " + Conteo_PS_PRODUCTO);
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_PRODUCTO_atributos_producto en mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_PRODUCTO_atributos_producto para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_PRODUCTO > 0)
                        {
                            Archivo_PS_PRODUCTO_atributos.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_PRODUCTO_atributos_producto_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_PRODUCTO_atributos_producto entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_PRODUCTO_atributos.Close();
            }

        }

        internal static void Extractor_PS_REGLA_ASIGANCION(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_REGLA_ASIGANCION");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_REGLA_ASIGANCION = null;

            int Conteo_PS_REGLA_ASIGANCION = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;
            int validacion_parametros = 0;

            string archivo = path + "PS_REGLA_ASIGANCION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_REGLA_ASIGANCION = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_REGLA_ASIGANCION");
                FilterDefinitionBuilder<BsonDocument> builderPS_REGLA_ASIGANCION = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_REGLA_ASIGANCION = builderPS_REGLA_ASIGANCION.Empty;

                if (tipo == "")
                {
                    filterPS_REGLA_ASIGANCION = builderPS_REGLA_ASIGANCION.Or(builderPS_REGLA_ASIGANCION.Eq("Actualizacion_Extractor", "1"), !builderPS_REGLA_ASIGANCION.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_REGLA_ASIGANCION = builderPS_REGLA_ASIGANCION.And(builderPS_REGLA_ASIGANCION.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_REGLA_ASIGANCION.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_REGLA_ASIGANCION = Col_PS.Find(filterPS_REGLA_ASIGANCION).ToList();

                if (consulta_PS_REGLA_ASIGANCION != null && consulta_PS_REGLA_ASIGANCION.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_REGLA_ASIGANCION encontrados " + consulta_PS_REGLA_ASIGANCION.Count.ToString());
                        foreach (BsonDocument itemPS_REGLA_ASIGANCION in consulta_PS_REGLA_ASIGANCION)
                        {
                            id_mongo = itemPS_REGLA_ASIGANCION.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_REGLA_ASIGANCION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_REGLA_ASIGANCION.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_REGLA_ASIGANCION.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_REGLA_ASIGANCION.Contains("_id") ? !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("_id")?.ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("_id").ToString().Length > 30 ? itemPS_REGLA_ASIGANCION.GetValue("_id").ToString().Substring(0, 29) : itemPS_REGLA_ASIGANCION.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("fecha_creacion") && !itemPS_REGLA_ASIGANCION.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("fecha_creacion").ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_REGLA_ASIGANCION.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_REGLA_ASIGANCION.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("usuario_creacion") && !itemPS_REGLA_ASIGANCION.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("usuario_creacion").ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_REGLA_ASIGANCION.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_REGLA_ASIGANCION.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("fecha_actualizacion") && !itemPS_REGLA_ASIGANCION.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("fecha_actualizacion").ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_REGLA_ASIGANCION.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_REGLA_ASIGANCION.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("usuario_modificacion") && !itemPS_REGLA_ASIGANCION.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("usuario_modificacion").ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_REGLA_ASIGANCION.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_REGLA_ASIGANCION.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("nombre_regla") && !itemPS_REGLA_ASIGANCION.GetValue("nombre_regla").IsBsonNull && !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("nombre_regla").ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("nombre_regla").ToString().Length > 50 ? itemPS_REGLA_ASIGANCION.GetValue("nombre_regla").ToString().Substring(0, 50) : itemPS_REGLA_ASIGANCION.GetValue("nombre_regla").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("descripcion") && !itemPS_REGLA_ASIGANCION.GetValue("descripcion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("descripcion").ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_REGLA_ASIGANCION.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_REGLA_ASIGANCION.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("id_grupo_asignado") ? !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("id_grupo_asignado")?.ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("id_grupo_asignado").ToString().Length > 30 ? itemPS_REGLA_ASIGANCION.GetValue("id_grupo_asignado").ToString().Substring(0, 29) : itemPS_REGLA_ASIGANCION.GetValue("id_grupo_asignado").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("grupo_asignado") && !itemPS_REGLA_ASIGANCION.GetValue("grupo_asignado").IsBsonNull && !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.GetValue("grupo_asignado").ToString()) ? (itemPS_REGLA_ASIGANCION.GetValue("grupo_asignado").ToString().Length > 50 ? itemPS_REGLA_ASIGANCION.GetValue("grupo_asignado").ToString().Substring(0, 50) : itemPS_REGLA_ASIGANCION.GetValue("grupo_asignado").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_REGLA_ASIGANCION.Contains("es_activo") && !itemPS_REGLA_ASIGANCION.GetValue("es_activo").IsBsonNull ? itemPS_REGLA_ASIGANCION.GetValue("es_activo").ToString().Length > 8 ? itemPS_REGLA_ASIGANCION.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_REGLA_ASIGANCION.GetValue("es_activo").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        if (itemPS_REGLA_ASIGANCION.Contains("validaciones_parametros") && !itemPS_REGLA_ASIGANCION.GetValue("validaciones_parametros").IsBsonNull && itemPS_REGLA_ASIGANCION.GetElement("validaciones_parametros").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                        {
                                            validacion_parametros += Extractor_PS_REGLA_ASIGANCION_validaciones_parametros(id_mongo);
                                        }                                        
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_REGLA_ASIGANCION Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_REGLA_ASIGANCION.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_REGLA_ASIGANCION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_REGLA_ASIGANCION++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_REGLA_ASIGANCION.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_REGLA_ASIGANCION.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_REGLA_ASIGANCION++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_REGLA_ASIGANCION ACTUALIZADA: " + itemPS_REGLA_ASIGANCION.GetValue("_id").ToString() + "Numero de PS_REGLA_ASIGANCION actializadas: " + Conteo_PS_REGLA_ASIGANCION);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_REGLA_ASIGANCION en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_REGLA_ASIGANCION para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_REGLA_ASIGANCION > 0)
                        {
                            Archivo_PS_REGLA_ASIGANCION.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_REGLA_ASIGANCION_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                if (validacion_parametros > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_REGLA_ASIGANCION_validaciones_parametros_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_REGLA_ASIGANCION entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_REGLA_ASIGANCION.Close();
            }

        }

        internal static int Extractor_PS_REGLA_ASIGANCION_validaciones_parametros(string id_mongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_REGLA_ASIGANCION_validaciones_parametros");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_REGLA_ASIGANCION_validaciones_parametros = null;

            int Conteo_PS_REGLA_ASIGANCION_validaciones_parametros = 0;
            string sTextoDescarga_validacion_parametros = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();


            string archivo = path + "PS_REGLA_ASIGANCION_validaciones_parametros_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_REGLA_ASIGANCION_validaciones_parametros = new StreamWriter(archivo, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_REGLA_ASIGANCION_validaciones_parametros = db.GetCollection<BsonDocument>("PS_REGLA_ASIGANCION");
                FilterDefinitionBuilder<BsonDocument> builderPS_REGLA_ASIGANCION_validaciones_parametros = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_REGLA_ASIGANCION_validaciones_parametros = builderPS_REGLA_ASIGANCION_validaciones_parametros.Empty;
                filterPS_REGLA_ASIGANCION_validaciones_parametros = builderPS_REGLA_ASIGANCION_validaciones_parametros.And(
                builderPS_REGLA_ASIGANCION_validaciones_parametros.Eq("_id", MongoDB.Bson.ObjectId.Parse(id_mongo)),
                builderPS_REGLA_ASIGANCION_validaciones_parametros.SizeGte("validaciones_parametros", 1));


                List<BsonDocument> consulta_PS_REGLA_ASIGANCION_VP = Col_PS_REGLA_ASIGANCION_validaciones_parametros.Find(filterPS_REGLA_ASIGANCION_validaciones_parametros).ToList();

                if (consulta_PS_REGLA_ASIGANCION_VP != null && consulta_PS_REGLA_ASIGANCION_VP.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_REGLA_ASIGANCION_validaciones_parametros encontrados " + consulta_PS_REGLA_ASIGANCION_VP.Count.ToString());
                        foreach (BsonDocument itemPS_REGLA_ASIGANCION in consulta_PS_REGLA_ASIGANCION_VP)
                        {
                            id_mongo = itemPS_REGLA_ASIGANCION.GetValue("_id").ToString();

                            sTextoDescarga_validacion_parametros = "";
                            List<BsonValue> consulta_PS_REGLA_ASIGANCION_validaciones_parametros = itemPS_REGLA_ASIGANCION.GetElement("validaciones_parametros").Value.AsBsonArray.AsQueryable().ToList();
                            
                            if (consulta_PS_REGLA_ASIGANCION_validaciones_parametros != null && consulta_PS_REGLA_ASIGANCION_validaciones_parametros.Count() > 0)
                            {
                                foreach (BsonValue itemPS_REGLA_ASIGANCION_validaciones_parametros in consulta_PS_REGLA_ASIGANCION_validaciones_parametros)
                                {
                                    try
                                    {
                                        try
                                        {
                                            List<BsonValue> consulta_PS_REGLA_ASIGANCION_parametros_validaciones = itemPS_REGLA_ASIGANCION.GetElement("valor_validacion").Value.AsBsonArray.AsQueryable().ToList();
                                            if (consulta_PS_REGLA_ASIGANCION_parametros_validaciones != null && consulta_PS_REGLA_ASIGANCION_parametros_validaciones.Count() > 0)
                                            {
                                                foreach (var item in consulta_PS_REGLA_ASIGANCION_parametros_validaciones)
                                                {
                                                    sTextoDescarga_validacion_parametros =
                                                    (itemPS_REGLA_ASIGANCION.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_REGLA_ASIGANCION.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_REGLA_ASIGANCION.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_REGLA_ASIGANCION.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                     "~|" + (itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().Contains("campo_validacion") && !itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("campo_validacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("campo_validacion").ToString()) ? (itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("campo_validacion").ToString().Length > 30 ? itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("campo_validacion").ToString().Substring(0, 29) : itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("campo_validacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                     "~|" + (itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().Contains("parametro_validacion") && !itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("parametro_validacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("parametro_validacion").ToString()) ? (itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("parametro_validacion").ToString().Length > 30 ? itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("parametro_validacion").ToString().Substring(0, 29) : itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().GetValue("parametro_validacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                     "~|" + (item.ToString());// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                     sTextoDescarga_validacion_parametros = sTextoDescarga_validacion_parametros.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                                }
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_REGLA_ASIGANCION_validaciones_parametros Id: " + id_mongo + "," + itemPS_REGLA_ASIGANCION_validaciones_parametros.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga_validacion_parametros != "")
                                            {
                                                Archivo_PS_REGLA_ASIGANCION_validaciones_parametros.WriteLine(sTextoDescarga_validacion_parametros);
                                                Console.WriteLine(sTextoDescarga_validacion_parametros);
                                                Conteo_PS_REGLA_ASIGANCION_validaciones_parametros++;
                                            }
                                            //Console.WriteLine("PS_REGLA_ASIGANCION_validaciones_parametros ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_REGLA_ASIGANCION_validaciones_parametros actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_REGLA_ASIGANCION_validaciones_parametros en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_REGLA_ASIGANCION_validaciones_parametros para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_REGLA_ASIGANCION_validaciones_parametros ACTUALIZADA: " + itemPS_REGLA_ASIGANCION.GetValue("_id").ToString() + "Numero de PS_REGLA_ASIGANCION_validaciones_parametros actializadas: " + Conteo_PS_REGLA_ASIGANCION_validaciones_parametros);
                            }

                        }

                        if (Conteo_PS_REGLA_ASIGANCION_validaciones_parametros > 0)
                        {
                            Archivo_PS_REGLA_ASIGANCION_validaciones_parametros.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_REGLA_ASIGANCION_validaciones_parametros entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_REGLA_ASIGANCION_validaciones_parametros;
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
                Archivo_PS_REGLA_ASIGANCION_validaciones_parametros.Close();

            }
            return Conteo_PS_REGLA_ASIGANCION_validaciones_parametros;
        } //Se ejecuta con Extractor_PS_REGLA_ASIGANCION()

        internal static void Extractor_PS_ROL(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_ROL");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_ROL = null;

            int Conteo_PS_ROL = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;
            int permisos = 0;

            string archivo = path + "PS_ROL_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_ROL = new StreamWriter(archivo, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS = db.GetCollection<BsonDocument>("PS_ROL");
                FilterDefinitionBuilder<BsonDocument> builderPS_ROL = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_ROL = builderPS_ROL.Empty;

                if (tipo == "")
                {
                    filterPS_ROL = builderPS_ROL.Or(builderPS_ROL.Eq("Actualizacion_Extractor", "1"), !builderPS_ROL.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);

                    filterPS_ROL = builderPS_ROL.And(builderPS_ROL.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_ROL.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_ROL = Col_PS.Find(filterPS_ROL).ToList();

                if (consulta_PS_ROL != null && consulta_PS_ROL.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_ROL encontrados " + consulta_PS_ROL.Count.ToString());
                        foreach (BsonDocument itemPS_ROL in consulta_PS_ROL)
                        {
                            id_mongo = itemPS_ROL.GetValue("_id").ToString();

                            sTextoDescarga = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_ROL.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_ROL.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_ROL.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga =
                                        (itemPS_ROL.Contains("_id") ? !string.IsNullOrEmpty(itemPS_ROL.GetValue("_id")?.ToString()) ? (itemPS_ROL.GetValue("_id").ToString().Length > 30 ? itemPS_ROL.GetValue("_id").ToString().Substring(0, 29) : itemPS_ROL.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ROL.Contains("fecha_creacion") && !itemPS_ROL.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ROL.GetValue("fecha_creacion").ToString()) ? (itemPS_ROL.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_ROL.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_ROL.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ROL.Contains("usuario_creacion") && !itemPS_ROL.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ROL.GetValue("usuario_creacion").ToString()) ? (itemPS_ROL.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_ROL.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_ROL.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ROL.Contains("fecha_actualizacion") && !itemPS_ROL.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ROL.GetValue("fecha_actualizacion").ToString()) ? (itemPS_ROL.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_ROL.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_ROL.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ROL.Contains("usuario_modificacion") && !itemPS_ROL.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ROL.GetValue("usuario_modificacion").ToString()) ? (itemPS_ROL.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_ROL.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_ROL.GetValue("usuario_modificacion").ToString()) : "") +// VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ROL.Contains("nombre") && !itemPS_ROL.GetValue("nombre").IsBsonNull && !string.IsNullOrEmpty(itemPS_ROL.GetValue("nombre").ToString()) ? (itemPS_ROL.GetValue("nombre").ToString().Length > 50 ? itemPS_ROL.GetValue("nombre").ToString().Substring(0, 50) : itemPS_ROL.GetValue("nombre").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ROL.Contains("descripcion") && !itemPS_ROL.GetValue("descripcion").IsBsonNull ? !string.IsNullOrEmpty(itemPS_ROL.GetValue("descripcion").ToString()) ? (itemPS_ROL.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Length > 500 ? itemPS_ROL.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ").Substring(0, 500) : itemPS_ROL.GetValue("descripcion").ToString().Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ")) : "" : "") + // VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_ROL.Contains("es_activo") && !itemPS_ROL.GetValue("es_activo").IsBsonNull ? itemPS_ROL.GetValue("es_activo").ToString().Length > 8 ? itemPS_ROL.GetValue("es_activo").ToString().Substring(0, 8) : itemPS_ROL.GetValue("es_activo").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        if (itemPS_ROL.Contains("permisos") && !itemPS_ROL.GetValue("permisos").IsBsonNull && itemPS_ROL.GetElement("permisos").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                        {
                                            permisos += Extractor_PS_ROL_permisos(id_mongo);
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_ROL Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        if (sTextoDescarga != "")
                                        {
                                            Archivo_PS_ROL.WriteLine(sTextoDescarga);
                                            if (pruebas == false)
                                            {
                                                if (tipo == "")
                                                {
                                                    Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_ROL.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                .Set("Fecha_extraccion", fechatemp.ToLocalTime()));
                                                    Conteo_PS_ROL++;
                                                }
                                                else if (tipo != "")
                                                {
                                                    if ((itemPS_ROL.GetValue("Actualizacion_Extractor") ?? "").ToString() != "0")
                                                    {
                                                        Col_PS.UpdateOne(Builders<BsonDocument>.Filter.Eq("_id", MongoDB.Bson.ObjectId.Parse(itemPS_ROL.GetValue("_id").ToString())), Builders<BsonDocument>.Update.Set("Actualizacion_Extractor", "0")
                                                                                                                                                                                                                    .Set("Fecha_extraccion", fechatemp.ToLocalTime()/*.ToString("dd/MM/yyyy")*/));
                                                        Conteo_PS_ROL++;
                                                    }

                                                }

                                            }
                                        }
                                        Console.WriteLine("PS_ROL ACTUALIZADA: " + itemPS_ROL.GetValue("_id").ToString() + "Numero de PS_ROL actializadas: " + Conteo_PS_ROL);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_ROL en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_ROL para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_ROL > 0)
                        {
                            Archivo_PS_ROL.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_ROL_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                if (permisos > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_ROL_permisos_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_ROL entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_ROL.Close();
            }

        }

        internal static int Extractor_PS_ROL_permisos(string id_mongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_ROL_permisos");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_ROL_permisos = null;

            int Conteo_PS_ROL_permisos = 0;
            string sTextoDescarga_permisos = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();


            string archivo = path + "PS_ROL_permisos_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_ROL_permisos = new StreamWriter(archivo, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_ROL_permisos = db.GetCollection<BsonDocument>("PS_ROL");
                FilterDefinitionBuilder<BsonDocument> builderPS_ROL_permisos = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_ROL_permisos = builderPS_ROL_permisos.Empty;
                filterPS_ROL_permisos = builderPS_ROL_permisos.And(
                builderPS_ROL_permisos.Eq("_id", MongoDB.Bson.ObjectId.Parse(id_mongo)),
                builderPS_ROL_permisos.SizeGte("permisos", 1));


                List<BsonDocument> consulta_PS_ROL_Permisos = Col_PS_ROL_permisos.Find(filterPS_ROL_permisos).ToList();

                if (consulta_PS_ROL_Permisos != null && consulta_PS_ROL_Permisos.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_ROL_permisos encontrados " + consulta_PS_ROL_Permisos.Count.ToString());
                        foreach (BsonDocument itemPS_ROL in consulta_PS_ROL_Permisos)
                        {
                            id_mongo = itemPS_ROL.GetValue("_id").ToString();

                            sTextoDescarga_permisos = "";
                            List<BsonValue> consulta_PS_ROL_permisos = itemPS_ROL.GetElement("permisos").Value.AsBsonArray.AsQueryable().ToList();

                            if (consulta_PS_ROL_permisos != null && consulta_PS_ROL_permisos.Count() > 0)
                            {
                                foreach (BsonValue itemPS_ROL_permisos in consulta_PS_ROL_permisos)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga_permisos =
                                            (itemPS_ROL.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_ROL.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_ROL.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_ROL.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_ROL.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                             "~|" + (itemPS_ROL_permisos.ToBsonDocument().Contains("funcionalidad") && !itemPS_ROL_permisos.ToBsonDocument().GetValue("funcionalidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_ROL_permisos.ToBsonDocument().GetValue("funcionalidad").ToString()) ? (itemPS_ROL_permisos.ToBsonDocument().GetValue("funcionalidad").ToString().Length > 30 ? itemPS_ROL_permisos.ToBsonDocument().GetValue("funcionalidad").ToString().Substring(0, 29) : itemPS_ROL_permisos.ToBsonDocument().GetValue("funcionalidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                             "~|" + (itemPS_ROL_permisos.ToBsonDocument().Contains("accion") && !itemPS_ROL_permisos.ToBsonDocument().GetValue("accion").IsBsonNull && !string.IsNullOrEmpty(itemPS_ROL_permisos.ToBsonDocument().GetValue("accion").ToString()) ? (itemPS_ROL_permisos.ToBsonDocument().GetValue("accion").ToString().Length > 30 ? itemPS_ROL_permisos.ToBsonDocument().GetValue("accion").ToString().Substring(0, 29) : itemPS_ROL_permisos.ToBsonDocument().GetValue("accion").ToString()) : "")+  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                             "~|" + (itemPS_ROL.Contains("permitido") && !itemPS_ROL.GetValue("permitido").IsBsonNull ? itemPS_ROL.GetValue("permitido").ToString().Length > 8 ? itemPS_ROL.GetValue("permitido").ToString().Substring(0, 8) : itemPS_ROL.GetValue("permitido").ToString() : "")+ //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                             "~|" + (itemPS_ROL.Contains("todos_elementos") && !itemPS_ROL.GetValue("todos_elementos").IsBsonNull ? itemPS_ROL.GetValue("todos_elementos").ToString().Length > 8 ? itemPS_ROL.GetValue("todos_elementos").ToString().Substring(0, 8) : itemPS_ROL.GetValue("todos_elementos").ToString() : ""); //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            sTextoDescarga_permisos = sTextoDescarga_permisos.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_ROL_permisos Id: " + id_mongo + "," + itemPS_ROL_permisos.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga_permisos != "")
                                            {
                                                Archivo_PS_ROL_permisos.WriteLine(sTextoDescarga_permisos);
                                                Console.WriteLine(sTextoDescarga_permisos);
                                                Conteo_PS_ROL_permisos++;
                                            }
                                            //Console.WriteLine("PS_ROL_permisos ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_ROL_permisos actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_ROL_permisos en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_ROL_permisos para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_ROL_permisos ACTUALIZADA: " + itemPS_ROL.GetValue("_id").ToString() + "Numero de PS_ROL_permisos actializadas: " + Conteo_PS_ROL_permisos);
                            }

                        }

                        if (Conteo_PS_ROL_permisos > 0)
                        {
                            Archivo_PS_ROL_permisos.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_ROL_permisos entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_ROL_permisos;
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
                Archivo_PS_ROL_permisos.Close();

            }
            return Conteo_PS_ROL_permisos;
        } //Se ejecuta con Extractor_PS_REGLA_ASIGANCION()

        internal static void Extractor_PS_SERVICIO_CLIENTE(string tipo = "")
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_SERVICIO_CLIENTE");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_SERVICIO_CLIENTE = null;

            int Conteo_PS_SERVICIO_CLIENTE = 0;
            int PS_SERVICIO_CLIENTE_CV = 0;
            string sTextoDescarga_SC = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            bool pruebas = true;
            int datos_adicionales = 0;
            int datos_adicionales_CV = 0;
            int Conteo_datos_adicionales_CV = 0;
            int Conteo_Configuracion_Servicio = 0;
            int Conteo_Configuracion_Servicio_CV = 0;
            int Configuracion_Servicio_CV = 0;
            int Conteo_Valores_Elementos = 0;
            int Conteo_Valores_Elementos_CV = 0;
            int Valores_Elementos_CV = 0;
            int Valores_Elementos = 0;
            string archivo_SC = path + "PS_SERVICIO_CLIENTE_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";
            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_SERVICIO_CLIENTE = new StreamWriter(archivo_SC, false, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_SERVICIO_CLIENTE = db.GetCollection<BsonDocument>("PS_SERVICIO_CLIENTE");
                FilterDefinitionBuilder<BsonDocument> builderPS_SERVICIO_CLIENTE = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_SERVICIO_CLIENTE = builderPS_SERVICIO_CLIENTE.Empty;

                if (tipo == "")
                {
                    filterPS_SERVICIO_CLIENTE = builderPS_SERVICIO_CLIENTE.Or(builderPS_SERVICIO_CLIENTE.Eq("Actualizacion_Extractor", "1"), !builderPS_SERVICIO_CLIENTE.Exists("Actualizacion_Extractor"));
                }
                else if (tipo != "full")
                {
                    DateTime fechaconsulta = DateTime.Parse(tipo);
                    
                    filterPS_SERVICIO_CLIENTE = builderPS_SERVICIO_CLIENTE.And(builderPS_SERVICIO_CLIENTE.Gte("Fecha_extraccion", fechaconsulta.Date), builderPS_SERVICIO_CLIENTE.Lt("Fecha_extraccion", fechaconsulta.Date.AddDays(1).AddSeconds(-1)));
                }

                List<BsonDocument> consulta_PS_SERVICIO_CLIENTE = Col_PS_SERVICIO_CLIENTE.Find(filterPS_SERVICIO_CLIENTE).ToList();

                if (consulta_PS_SERVICIO_CLIENTE != null && consulta_PS_SERVICIO_CLIENTE.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_SERVICIO_CLIENTE encontrados " + consulta_PS_SERVICIO_CLIENTE.Count.ToString());
                        foreach (BsonDocument itemPS_SERVICIO_CLIENTE in consulta_PS_SERVICIO_CLIENTE)
                        {
                            id_mongo = itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString();

                            sTextoDescarga_SC = "";

                            try
                            {
                                if (!string.IsNullOrEmpty(id_mongo)
                                    && (!itemPS_SERVICIO_CLIENTE.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || itemPS_SERVICIO_CLIENTE.ToBsonDocument().Contains("Actualizacion_Extractor")
                                    || (itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("Actualizacion_Extractor").IsBsonNull))
                                    )
                                {
                                    try
                                    {
                                        sTextoDescarga_SC =
                                        (itemPS_SERVICIO_CLIENTE.Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("fecha_creacion") && !itemPS_SERVICIO_CLIENTE.GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("fecha_creacion").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("fecha_creacion").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("fecha_creacion").ToString().Substring(0, 30) : itemPS_SERVICIO_CLIENTE.GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("usuario_creacion") && !itemPS_SERVICIO_CLIENTE.GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("usuario_creacion").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("usuario_creacion").ToString().Length > 50 ? itemPS_SERVICIO_CLIENTE.GetValue("usuario_creacion").ToString().Substring(0, 50) : itemPS_SERVICIO_CLIENTE.GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("fecha_actualizacion") && !itemPS_SERVICIO_CLIENTE.GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("fecha_actualizacion").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("fecha_actualizacion").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemPS_SERVICIO_CLIENTE.GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("usuario_modificacion") && !itemPS_SERVICIO_CLIENTE.GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("usuario_modificacion").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("usuario_modificacion").ToString().Length > 50 ? itemPS_SERVICIO_CLIENTE.GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemPS_SERVICIO_CLIENTE.GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("estado") && !itemPS_SERVICIO_CLIENTE.GetValue("estado").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("estado").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("estado").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("estado").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("estado").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("ancho_banda") && !itemPS_SERVICIO_CLIENTE.GetValue("ancho_banda").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("ancho_banda").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("ancho_banda").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("ancho_banda").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("ancho_banda").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("cuenta_cliente") && !itemPS_SERVICIO_CLIENTE.GetValue("cuenta_cliente").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("cuenta_cliente").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("cuenta_cliente").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("cuenta_cliente").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("cuenta_cliente").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("nit") && !itemPS_SERVICIO_CLIENTE.GetValue("nit").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("nit").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("nit").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("nit").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("nit").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("id_servicio") && !itemPS_SERVICIO_CLIENTE.GetValue("id_servicio").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("id_servicio").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("id_servicio").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("id_servicio").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("id_servicio").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("nombre_producto") && !itemPS_SERVICIO_CLIENTE.GetValue("nombre_producto").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("nombre_producto").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("nombre_producto").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("nombre_producto").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("nombre_producto").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("plan") && !itemPS_SERVICIO_CLIENTE.GetValue("plan").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("plan").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("plan").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("plan").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("plan").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("sucursal") && !itemPS_SERVICIO_CLIENTE.GetValue("sucursal").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("sucursal").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("sucursal").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("sucursal").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("sucursal").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("ciudad") && !itemPS_SERVICIO_CLIENTE.GetValue("ciudad").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("ciudad").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("ciudad").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("ciudad").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("ciudad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("disponibilidad_servicio") && !itemPS_SERVICIO_CLIENTE.GetValue("disponibilidad_servicio").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("disponibilidad_servicio").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("disponibilidad_servicio").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("disponibilidad_servicio").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("disponibilidad_servicio").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("servicio_etb") && !itemPS_SERVICIO_CLIENTE.GetValue("servicio_etb").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("servicio_etb").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("servicio_etb").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("servicio_etb").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("servicio_etb").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("cuenta_facturacion") && !itemPS_SERVICIO_CLIENTE.GetValue("cuenta_facturacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("cuenta_facturacion").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("cuenta_facturacion").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("cuenta_facturacion").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("cuenta_facturacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("aliado_colaborador") && !itemPS_SERVICIO_CLIENTE.GetValue("aliado_colaborador").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("aliado_colaborador").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("aliado_colaborador").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("aliado_colaborador").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("aliado_colaborador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("proveedor_ultima_milla") && !itemPS_SERVICIO_CLIENTE.GetValue("proveedor_ultima_milla").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("proveedor_ultima_milla").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("proveedor_ultima_milla").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("proveedor_ultima_milla").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("proveedor_ultima_milla").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("medio_ultima_milla") && !itemPS_SERVICIO_CLIENTE.GetValue("medio_ultima_milla").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("medio_ultima_milla").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("medio_ultima_milla").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("medio_ultima_milla").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("medio_ultima_milla").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("external_service_id") && !itemPS_SERVICIO_CLIENTE.GetValue("external_service_id").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("external_service_id").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("external_service_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("external_service_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("external_service_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("numero_conexion") && !itemPS_SERVICIO_CLIENTE.GetValue("numero_conexion").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("numero_conexion").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("numero_conexion").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("numero_conexion").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("numero_conexion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("numero_aprovisionamiento") && !itemPS_SERVICIO_CLIENTE.GetValue("numero_aprovisionamiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("numero_aprovisionamiento").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("numero_aprovisionamiento").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("numero_aprovisionamiento").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("numero_aprovisionamiento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("numero_viabilidad") && !itemPS_SERVICIO_CLIENTE.GetValue("numero_viabilidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("numero_viabilidad").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("numero_viabilidad").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("numero_viabilidad").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("numero_viabilidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("opcion_respuesta_viabilidad") && !itemPS_SERVICIO_CLIENTE.GetValue("opcion_respuesta_viabilidad").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("opcion_respuesta_viabilidad").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("opcion_respuesta_viabilidad").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("opcion_respuesta_viabilidad").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("opcion_respuesta_viabilidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("fecha_inicio_facturacion") && !itemPS_SERVICIO_CLIENTE.GetValue("fecha_inicio_facturacion").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("fecha_inicio_facturacion").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("fecha_inicio_facturacion").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("fecha_inicio_facturacion").ToString().Substring(0, 30) : itemPS_SERVICIO_CLIENTE.GetValue("fecha_inicio_facturacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("version") && !itemPS_SERVICIO_CLIENTE.GetValue("version").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("version").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("version").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("version").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("version").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        "~|" + (itemPS_SERVICIO_CLIENTE.Contains("id_Aprovisionamiento") && !itemPS_SERVICIO_CLIENTE.GetValue("id_Aprovisionamiento").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("id_Aprovisionamiento").ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("id_Aprovisionamiento").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("id_Aprovisionamiento").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("id_Aprovisionamiento").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                        //"~|" + (itemPS_SERVICIO_CLIENTE.Contains("historico_modificaciones") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.GetValue("historico_modificaciones")?.ToString()) ? (itemPS_SERVICIO_CLIENTE.GetValue("historico_modificaciones").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.GetValue("historico_modificaciones").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.GetValue("historico_modificaciones").ToString()) : "" : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,                                        
                                        sTextoDescarga_SC = sTextoDescarga_SC.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        if (itemPS_SERVICIO_CLIENTE.Contains("datos_adicionales_servicio") && !itemPS_SERVICIO_CLIENTE.GetValue("datos_adicionales_servicio").IsBsonNull && itemPS_SERVICIO_CLIENTE.GetElement("datos_adicionales_servicio").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                        {
                                            datos_adicionales += Extractor_PS_SERVICIO_CLIENTE_datos_adicionales_servicio(id_mongo);
                                        }
                                        if (itemPS_SERVICIO_CLIENTE.Contains("configuracion_servicio") && !itemPS_SERVICIO_CLIENTE.GetValue("configuracion_servicio").IsBsonNull && itemPS_SERVICIO_CLIENTE.GetElement("configuracion_servicio").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                        {
                                            Conteo_Configuracion_Servicio += Extractor_PS_SERVICIO_CLIENTE_configuracion_servicio(id_mongo, out Valores_Elementos);
                                            Conteo_Valores_Elementos += Valores_Elementos;
                                        }
                                        if (itemPS_SERVICIO_CLIENTE.Contains("control_versiones") && !itemPS_SERVICIO_CLIENTE.GetValue("control_versiones").IsBsonNull && itemPS_SERVICIO_CLIENTE.GetElement("control_versiones").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                        {
                                            PS_SERVICIO_CLIENTE_CV += Extractor_PS_SERVICIO_CLIENTE_CV(id_mongo, out Configuracion_Servicio_CV, out Valores_Elementos_CV, out datos_adicionales_CV);
                                            Conteo_Configuracion_Servicio_CV += Configuracion_Servicio_CV;
                                            Conteo_Valores_Elementos_CV += Valores_Elementos_CV;
                                            Conteo_datos_adicionales_CV += datos_adicionales_CV;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_SERVICIO_CLIENTE Id: " + id_mongo);
                                        continue;
                                    }
                                    // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                    try
                                    {
                                        Conteo_PS_SERVICIO_CLIENTE++;
                                        Archivo_PS_SERVICIO_CLIENTE.WriteLine(sTextoDescarga_SC);
                                        Console.WriteLine("PS_SERVICIO_CLIENTE ACTUALIZADA: " + itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE actializadas: " + Conteo_PS_SERVICIO_CLIENTE);
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_SERVICIO_CLIENTE en mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_SERVICIO_CLIENTE para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_SERVICIO_CLIENTE > 0)
                        {
                            Archivo_PS_SERVICIO_CLIENTE.Close();
                            if (pruebas == false)
                            {
                                //PublicarArchivo.PublicarArchivoExtractores("PS_SERVICIO_CLIENTE_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                if (Conteo_Configuracion_Servicio > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_SERVICIO_CLIENTE_configuracion_servicio_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                                if (Conteo_Valores_Elementos > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_SERVICIO_CLIENTE_valores_elementos_configuracion_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                                if (datos_adicionales > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_SERVICIO_CLIENTE_datos_adicionales_servicio_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                                if (PS_SERVICIO_CLIENTE_CV > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_SERVICIO_CLIENTE_CV_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                                if (Conteo_Configuracion_Servicio_CV > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_SERVICIO_CLIENTE_CV_CS_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");

                                }
                                if (Conteo_Valores_Elementos_CV > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");
                                }
                                if (Conteo_datos_adicionales_CV > 0)
                                {
                                    //PublicarArchivo.PublicarArchivoExtractores("PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt");

                                }
                               

                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_SERVICIO_CLIENTE entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_SERVICIO_CLIENTE.Close();
            }

        }

        internal static int Extractor_PS_SERVICIO_CLIENTE_configuracion_servicio(string idmongo, out int valores_elementos) 
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_SERVICIO_CLIENTE_configuracion_servicio");
            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());
            StreamWriter Archivo_PS_SERVICIO_CLIENTE_configuracion_servicio = null;
            int Conteo_PS_SERVICIO_CLIENTE = 0;
            valores_elementos = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            string archivo_CS = path + "PS_SERVICIO_CLIENTE_configuracion_servicio_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";
            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_SERVICIO_CLIENTE_configuracion_servicio = new StreamWriter(archivo_CS, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_SERVICIO_CLIENTE_CS = db.GetCollection<BsonDocument>("PS_SERVICIO_CLIENTE");
                FilterDefinitionBuilder<BsonDocument> builderPS_SERVICIO_CLIENTE_CS = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_SERVICIO_CLIENTE_CS = builderPS_SERVICIO_CLIENTE_CS.Empty;
                filterPS_SERVICIO_CLIENTE_CS = builderPS_SERVICIO_CLIENTE_CS.Eq("_id", MongoDB.Bson.ObjectId.Parse(idmongo));
                List<BsonDocument> consulta_PS_SERVICIO_CLIENTE_CS = Col_PS_SERVICIO_CLIENTE_CS.Find(filterPS_SERVICIO_CLIENTE_CS).ToList();

                if (consulta_PS_SERVICIO_CLIENTE_CS != null && consulta_PS_SERVICIO_CLIENTE_CS.Count() > 0)
                {
                    try
                    {
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PQR 
                        Console.WriteLine("Registros en la coleccion de PS_SERVICIO_CLIENTE_configuracion_servicio encontrados " + consulta_PS_SERVICIO_CLIENTE_CS.Count.ToString());
                        foreach (BsonDocument itemPS_SERVICIO_CLIENTE_CS in consulta_PS_SERVICIO_CLIENTE_CS)
                        {
                            id_mongo = itemPS_SERVICIO_CLIENTE_CS.GetValue("_id").ToString();

                            sTextoDescarga = "";
                            List<BsonValue> consulta_PS_SERVICIO_CLIENTE_configuracion_servicio = itemPS_SERVICIO_CLIENTE_CS.GetElement("configuracion_servicio").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_SERVICIO_CLIENTE_configuracion_servicio != null && consulta_PS_SERVICIO_CLIENTE_configuracion_servicio.Count() > 0)
                            {
                                foreach (BsonValue itemconfiguracion_servicio in consulta_PS_SERVICIO_CLIENTE_configuracion_servicio)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga =
                                            (itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("_id") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("_id").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemconfiguracion_servicio.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("fecha_creacion") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("usuario_creacion") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_creacion").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_creacion").ToString().Length > 50 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_creacion").ToString().Substring(0, 50) : itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("fecha_actualizacion") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Length > 30 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Substring(0, 30) : itemconfiguracion_servicio.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("usuario_modificacion") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("id_agrupador") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("id_agrupador").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("id_agrupador").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("id_agrupador").ToString().Length > 30 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("id_agrupador").ToString().Substring(0, 29) : itemconfiguracion_servicio.ToBsonDocument().GetValue("id_agrupador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("nombre_agrupador") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_agrupador").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_agrupador").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_agrupador").ToString().Length > 30 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_agrupador").ToString().Substring(0, 29) : itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_agrupador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("nombre_elemento") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_elemento").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_elemento").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_elemento").ToString().Length > 30 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_elemento").ToString().Substring(0, 29) : itemconfiguracion_servicio.ToBsonDocument().GetValue("nombre_elemento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("cantidad") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("cantidad").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("cantidad").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("cantidad").ToString().Length > 30 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("cantidad").ToString().Substring(0, 29) : itemconfiguracion_servicio.ToBsonDocument().GetValue("cantidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("tipo_elemento") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("tipo_elemento").IsBsonNull && !string.IsNullOrEmpty(itemconfiguracion_servicio.ToBsonDocument().GetValue("tipo_elemento").ToString()) ? (itemconfiguracion_servicio.ToBsonDocument().GetValue("tipo_elemento").ToString().Length > 30 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("tipo_elemento").ToString().Substring(0, 29) : itemconfiguracion_servicio.ToBsonDocument().GetValue("tipo_elemento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                            "~|" + (itemconfiguracion_servicio.ToBsonDocument().Contains("inventario_etb") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("inventario_etb").IsBsonNull ? itemconfiguracion_servicio.ToBsonDocument().GetValue("inventario_etb").ToString().Length > 8 ? itemconfiguracion_servicio.ToBsonDocument().GetValue("inventario_etb").ToString().Substring(0, 8) : itemconfiguracion_servicio.ToBsonDocument().GetValue("inventario_etb").ToString() : "");
                                            sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            if (itemconfiguracion_servicio.ToBsonDocument().Contains("valores_elementos_configuracion") && !itemconfiguracion_servicio.ToBsonDocument().GetValue("valores_elementos_configuracion").IsBsonNull && itemconfiguracion_servicio.ToBsonDocument().GetElement("valores_elementos_configuracion").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                            {
                                                valores_elementos += Extractor_PS_SERVICIO_CLIENTE_valores_elementos_configuracion(id_mongo, itemconfiguracion_servicio.ToBsonDocument().GetValue("_id").ToString());
                                            }

                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_SERVICIO_CLIENTE_configuracion_servicio Id: " + id_mongo + "," + itemconfiguracion_servicio.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga != "")
                                            {
                                                Archivo_PS_SERVICIO_CLIENTE_configuracion_servicio.WriteLine(sTextoDescarga);
                                                Conteo_PS_SERVICIO_CLIENTE++;
                                            }
                                            Console.WriteLine("PS_SERVICIO_CLIENTE_configuracion_servicio ACTUALIZADA: " + itemPS_SERVICIO_CLIENTE_CS.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_configuracion_servicio actializadas: " + Conteo_PS_SERVICIO_CLIENTE);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_SERVICIO_CLIENTE_configuracion_servicio en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_SERVICIO_CLIENTE_configuracion_servicio para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_SERVICIO_CLIENTE > 0)
                        {
                            Archivo_PS_SERVICIO_CLIENTE_configuracion_servicio.Close();                            
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_SERVICIO_CLIENTE_configuracion_servicio entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_SERVICIO_CLIENTE_configuracion_servicio.Close();
            }
            return Conteo_PS_SERVICIO_CLIENTE;
        } //Se ejecuta con Extractor_PS_SERVICIO_CLIENTE()

        internal static int Extractor_PS_SERVICIO_CLIENTE_valores_elementos_configuracion(string idmongo, string id_mongo_registro)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_SERVICIO_CLIENTE_valores_elementos_configuracion");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());
            StreamWriter Archivo_PS_SERVICIO_CLIENTE_valores_elementos_configuracion = null;
            int Conteo_PS_SERVICIO_CLIENTE = 0;
            string sTextoDescarga_SC = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();         

            string archivo_VC = path + "PS_SERVICIO_CLIENTE_valores_elementos_configuracion_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_SERVICIO_CLIENTE_valores_elementos_configuracion = new StreamWriter(archivo_VC, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_SERVICIO_CLIENTE = db.GetCollection<BsonDocument>("PS_SERVICIO_CLIENTE");
                FilterDefinitionBuilder<BsonDocument> builderPS_SERVICIO_CLIENTE = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_SERVICIO_CLIENTE = builderPS_SERVICIO_CLIENTE.Empty;
                filterPS_SERVICIO_CLIENTE = builderPS_SERVICIO_CLIENTE.Eq("_id", MongoDB.Bson.ObjectId.Parse(idmongo));
                List<BsonDocument> consulta_PS_SERVICIO_CLIENTE_CS = Col_PS_SERVICIO_CLIENTE.Find(filterPS_SERVICIO_CLIENTE).ToList();

                if (consulta_PS_SERVICIO_CLIENTE_CS != null && consulta_PS_SERVICIO_CLIENTE_CS.Count() > 0)
                {
                    try
                    {
                        foreach (BsonDocument itemPS_SERVICIO_CLIENTE_CS in consulta_PS_SERVICIO_CLIENTE_CS)
                        {
                            id_mongo = itemPS_SERVICIO_CLIENTE_CS.GetValue("_id").ToString();
                            
                            List<BsonValue> consulta_PS_SERVICIO_CLIENTE_configuracion_servicio = itemPS_SERVICIO_CLIENTE_CS.GetElement("configuracion_servicio").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_SERVICIO_CLIENTE_configuracion_servicio != null && consulta_PS_SERVICIO_CLIENTE_configuracion_servicio.Count() > 0)
                            {
                                foreach (BsonValue item_configuracion_servicio in consulta_PS_SERVICIO_CLIENTE_configuracion_servicio)
                                {
                                    try
                                    {
                                        List<BsonValue> consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion = item_configuracion_servicio.ToBsonDocument().GetElement("valores_elementos_configuracion").Value.AsBsonArray.AsQueryable().ToList();
                                        if (consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion != null && consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion.Count() > 0)
                                        {
                                            // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PS_SERVICIO_CLIENTE_valores_elementos_configuracion 
                                            Console.WriteLine("Registros en la coleccion de PS_SERVICIO_CLIENTE_valores_elementos_configuracion encontrados " + consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion.Count.ToString());
                                            foreach (var item_elementos_configuracion in consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion)
                                            {
                                                try
                                                {
                                                    sTextoDescarga_SC = "";
                                                    if (item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString() == id_mongo_registro)
                                                    {
                                                        sTextoDescarga_SC =
                                                            (itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_CS.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                            "~|" + (item_configuracion_servicio.ToBsonDocument().Contains("_id") && !item_configuracion_servicio.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString()) ? (item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                            "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("_id") && !item_elementos_configuracion.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                            "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("fecha_creacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                            "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("usuario_creacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString().Length > 50 ? item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString().Substring(0, 50) : item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                            "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("fecha_actualizacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Substring(0, 30) : item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                            "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("usuario_modificacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                            "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("marca") && !item_elementos_configuracion.ToBsonDocument().GetValue("marca").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("marca").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("marca").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("marca").ToString().Substring(0, 29) : item_elementos_configuracion.ToBsonDocument().GetValue("marca").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                            "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("referencia") && !item_elementos_configuracion.ToBsonDocument().GetValue("referencia").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("referencia").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("referencia").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("referencia").ToString().Substring(0, 29) : item_elementos_configuracion.ToBsonDocument().GetValue("referencia").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                            "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("valor_asignacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").ToString().Substring(0, 29) : item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                        sTextoDescarga_SC = sTextoDescarga_SC.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                                    }
                                                    
                                                }
                                                catch (Exception ex)
                                                {
                                                    string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                    prcManejoErrores objError = new prcManejoErrores();
                                                    objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_SERVICIO_CLIENTE_valores_elementos_configuracion Id: " + id_mongo + "," + item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString());
                                                    continue;
                                                }
                                                // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                                try
                                                {
                                                    if (sTextoDescarga_SC != "")
                                                    {
                                                        Archivo_PS_SERVICIO_CLIENTE_valores_elementos_configuracion.WriteLine(sTextoDescarga_SC);
                                                        Conteo_PS_SERVICIO_CLIENTE++;
                                                    }
                                                    Console.WriteLine("PS_SERVICIO_CLIENTE_valores_elementos_configuracion ACTUALIZADA: " + itemPS_SERVICIO_CLIENTE_CS.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_valores_elementos_configuracion actializadas: " + Conteo_PS_SERVICIO_CLIENTE);
                                                }
                                                catch (Exception ex)
                                                {
                                                    string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                    prcManejoErrores objError = new prcManejoErrores();
                                                    objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_SERVICIO_CLIENTE_valores_elementos_configuracion en mongo Id: " + id_mongo);
                                                    continue;
                                                }
                                            }
                                        }

                                        
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_SERVICIO_CLIENTE_valores_elementos_configuracion para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                            }

                        }

                        if (Conteo_PS_SERVICIO_CLIENTE > 0)
                        {
                            Archivo_PS_SERVICIO_CLIENTE_valores_elementos_configuracion.Close();                            
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_SERVICIO_CLIENTE_valores_elementos_configuracion entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_SERVICIO_CLIENTE_valores_elementos_configuracion.Close();
            }
            return Conteo_PS_SERVICIO_CLIENTE;
        }  //Se ejecuta con Extractor_PS_SERVICIO_CLIENTE_configuracion_servicio()

        internal static int Extractor_PS_SERVICIO_CLIENTE_datos_adicionales_servicio(string idmongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_SERVICIO_CLIENTE_datos_adicionales_servicio");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());
            StreamWriter Archivo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio = null;
            int Conteo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio = 0;
            string sTextoDescarga_datos_adicionales_servicio = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            string archivo_datos_adicionales_servicio = path + "PS_SERVICIO_CLIENTE_datos_adicionales_servicio_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio = new StreamWriter(archivo_datos_adicionales_servicio, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_SERVICIO_CLIENTE_datos_adicionales_servicio = db.GetCollection<BsonDocument>("PS_SERVICIO_CLIENTE");
                FilterDefinitionBuilder<BsonDocument> builderPS_SERVICIO_CLIENTE_datos_adicionales_servicio = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_SERVICIO_CLIENTE_datos_adicionales_servicio = builderPS_SERVICIO_CLIENTE_datos_adicionales_servicio.Empty;
                filterPS_SERVICIO_CLIENTE_datos_adicionales_servicio = builderPS_SERVICIO_CLIENTE_datos_adicionales_servicio.And(
                builderPS_SERVICIO_CLIENTE_datos_adicionales_servicio.Eq("_id", MongoDB.Bson.ObjectId.Parse(idmongo)),
                builderPS_SERVICIO_CLIENTE_datos_adicionales_servicio.SizeGte("datos_adicionales_servicio", 1));


                List<BsonDocument> consulta_PS_SERVICIO_CLIENTE_datos = Col_PS_SERVICIO_CLIENTE_datos_adicionales_servicio.Find(filterPS_SERVICIO_CLIENTE_datos_adicionales_servicio).ToList();

                if (consulta_PS_SERVICIO_CLIENTE_datos != null && consulta_PS_SERVICIO_CLIENTE_datos.Count() > 0)
                {
                    try
                    {
                        foreach (BsonDocument itemPS_SERVICIO_CLIENTE in consulta_PS_SERVICIO_CLIENTE_datos)
                        {
                            id_mongo = itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString();

                            sTextoDescarga_datos_adicionales_servicio = "";
                            List<BsonValue> consulta_PS_SERVICIO_CLIENTE_datos_adicionales_servicio = itemPS_SERVICIO_CLIENTE.GetElement("datos_adicionales_servicio").Value.AsBsonArray.AsQueryable().ToList();

                            if (consulta_PS_SERVICIO_CLIENTE_datos_adicionales_servicio != null && consulta_PS_SERVICIO_CLIENTE_datos_adicionales_servicio.Count() > 0)
                            {
                                // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PS_SERVICIO_CLIENTE_datos_adicionales_servicio 
                                Console.WriteLine("Registros en la coleccion de PS_SERVICIO_CLIENTE_datos_adicionales_servicio encontrados " + consulta_PS_SERVICIO_CLIENTE_datos_adicionales_servicio.Count.ToString());
                                foreach (BsonValue itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio in consulta_PS_SERVICIO_CLIENTE_datos_adicionales_servicio)
                                {
                                    try
                                    {
                                        try
                                        {
                                            sTextoDescarga_datos_adicionales_servicio =
                                            (itemPS_SERVICIO_CLIENTE.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                             "~|" + (itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().Contains("identificador") && !itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").ToString()) ? (itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                             "~|" + (itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().Contains("valor") && !itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").ToString()) ? (itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                            sTextoDescarga_datos_adicionales_servicio = sTextoDescarga_datos_adicionales_servicio.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_SERVICIO_CLIENTE_datos_adicionales_servicio Id: " + id_mongo + "," + itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().ToString());
                                            continue;
                                        }
                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                        try
                                        {
                                            if (sTextoDescarga_datos_adicionales_servicio != "")
                                            {
                                                Archivo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio.WriteLine(sTextoDescarga_datos_adicionales_servicio);
                                                Console.WriteLine(sTextoDescarga_datos_adicionales_servicio);
                                                Conteo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio++;
                                            }
                                            //Console.WriteLine("PS_SERVICIO_CLIENTE_datos_adicionales_servicio ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_datos_adicionales_servicio actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                        }
                                        catch (Exception ex)
                                        {
                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                            prcManejoErrores objError = new prcManejoErrores();
                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_SERVICIO_CLIENTE_datos_adicionales_servicio en mongo Id: " + id_mongo);
                                            continue;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                        prcManejoErrores objError = new prcManejoErrores();
                                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_SERVICIO_CLIENTE_datos_adicionales_servicio para el procesamiento de registros de mongo Id: " + id_mongo);
                                        continue;
                                    }
                                }
                                Console.WriteLine("PS_SERVICIO_CLIENTE_datos_adicionales_servicio ACTUALIZADA: " + itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_datos_adicionales_servicio actializadas: " + Conteo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio);
                            }

                        }

                        if (Conteo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio > 0)
                        {
                            Archivo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_SERVICIO_CLIENTE_datos_adicionales_servicio entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio;
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
                Archivo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio.Close();
            }
            return Conteo_PS_SERVICIO_CLIENTE_datos_adicionales_servicio;
        } //Se ejecuta con Extractor_PS_SERVICIO_CLIENTE()

        internal static int Extractor_PS_SERVICIO_CLIENTE_CV(string idmongo, out int configuracion_servicio_CV,out int valores_elementos_CV, out int datos_adicionales_CV)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_SERVICIO_CLIENTE_CV");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());

            StreamWriter Archivo_PS_SERVICIO_CLIENTE_CV = null;

            int Conteo_PS_SERVICIO_CLIENTE_CV = 0;
            string sTextoDescarga_cv = "";
            string id_mongo = "";            
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            datos_adicionales_CV = 0;
            valores_elementos_CV = 0;
            configuracion_servicio_CV = 0;

            string archivo_CV = path + "PS_SERVICIO_CLIENTE_CV_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_SERVICIO_CLIENTE_CV = new StreamWriter(archivo_CV, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_SERVICIO_CLIENTE_CV = db.GetCollection<BsonDocument>("PS_SERVICIO_CLIENTE");
                FilterDefinitionBuilder<BsonDocument> builderPS_SERVICIO_CLIENTE_CV = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_SERVICIO_CLIENTE_CV = builderPS_SERVICIO_CLIENTE_CV.Empty;
                filterPS_SERVICIO_CLIENTE_CV = builderPS_SERVICIO_CLIENTE_CV.Eq("_id", MongoDB.Bson.ObjectId.Parse(idmongo));
                
                List<BsonDocument> consulta_PS_SERVICIO_CLIENTE_CV = Col_PS_SERVICIO_CLIENTE_CV.Find(filterPS_SERVICIO_CLIENTE_CV).ToList();

                if (consulta_PS_SERVICIO_CLIENTE_CV != null && consulta_PS_SERVICIO_CLIENTE_CV.Count() > 0)
                {
                    try
                    {
                        foreach (BsonDocument itemPS_SERVICIO_CLIENTE_CV in consulta_PS_SERVICIO_CLIENTE_CV)
                        {
                            id_mongo = itemPS_SERVICIO_CLIENTE_CV.GetValue("_id").ToString();
                            sTextoDescarga_cv = "";
                            try
                            {
                                try
                                {
                                    List<BsonValue> consulta_PS_SERVICIO_CLIENTE_CVI = itemPS_SERVICIO_CLIENTE_CV.GetElement("control_versiones").Value.AsBsonArray.AsQueryable().ToList();
                                    if (consulta_PS_SERVICIO_CLIENTE_CVI != null && consulta_PS_SERVICIO_CLIENTE_CVI.Count() > 0)
                                    {
                                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PS_SERVICIO_CLIENTE_CV
                                        Console.WriteLine("Registros en la coleccion de PS_SERVICIO_CLIENTE_CV encontrados " + consulta_PS_SERVICIO_CLIENTE_CV.Count.ToString());
                                        foreach (var item_CVI in consulta_PS_SERVICIO_CLIENTE_CVI)
                                        {
                                            sTextoDescarga_cv =
                                                (itemPS_SERVICIO_CLIENTE_CV.Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_CV.GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE_CV.GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_CV.GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_CV.GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (item_CVI.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("_id")?.ToString()) ? (item_CVI.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (item_CVI.ToBsonDocument().Contains("fecha_creacion") && !item_CVI.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (item_CVI.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : item_CVI.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (item_CVI.ToBsonDocument().Contains("usuario_creacion") && !item_CVI.ToBsonDocument().GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("usuario_creacion").ToString()) ? (item_CVI.ToBsonDocument().GetValue("usuario_creacion").ToString().Length > 50 ? item_CVI.ToBsonDocument().GetValue("usuario_creacion").ToString().Substring(0, 50) : item_CVI.ToBsonDocument().GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (item_CVI.ToBsonDocument().Contains("fecha_actualizacion") && !item_CVI.ToBsonDocument().GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) ? (item_CVI.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Substring(0, 30) : item_CVI.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (item_CVI.ToBsonDocument().Contains("usuario_modificacion") && !item_CVI.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (item_CVI.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? item_CVI.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : item_CVI.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (item_CVI.ToBsonDocument().Contains("estado") && !item_CVI.ToBsonDocument().GetValue("estado").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("estado").ToString()) ? (item_CVI.ToBsonDocument().GetValue("estado").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("estado").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("estado").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("ancho_banda") && !item_CVI.ToBsonDocument().GetValue("ancho_banda").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("ancho_banda").ToString()) ? (item_CVI.ToBsonDocument().GetValue("ancho_banda").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("ancho_banda").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("ancho_banda").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("cuenta_cliente") && !item_CVI.ToBsonDocument().GetValue("cuenta_cliente").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("cuenta_cliente").ToString()) ? (item_CVI.ToBsonDocument().GetValue("cuenta_cliente").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("cuenta_cliente").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("cuenta_cliente").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("nit") && !item_CVI.ToBsonDocument().GetValue("nit").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("nit").ToString()) ? (item_CVI.ToBsonDocument().GetValue("nit").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("nit").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("nit").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("id_servicio") && !item_CVI.ToBsonDocument().GetValue("id_servicio").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("id_servicio").ToString()) ? (item_CVI.ToBsonDocument().GetValue("id_servicio").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("id_servicio").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("id_servicio").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("nombre_producto") && !item_CVI.ToBsonDocument().GetValue("nombre_producto").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("nombre_producto").ToString()) ? (item_CVI.ToBsonDocument().GetValue("nombre_producto").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("nombre_producto").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("nombre_producto").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("plan") && !item_CVI.ToBsonDocument().GetValue("plan").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("plan").ToString()) ? (item_CVI.ToBsonDocument().GetValue("plan").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("plan").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("plan").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("sucursal") && !item_CVI.ToBsonDocument().GetValue("sucursal").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("sucursal").ToString()) ? (item_CVI.ToBsonDocument().GetValue("sucursal").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("sucursal").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("sucursal").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("ciudad") && !item_CVI.ToBsonDocument().GetValue("ciudad").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("ciudad").ToString()) ? (item_CVI.ToBsonDocument().GetValue("ciudad").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("ciudad").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("ciudad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("disponibilidad_servicio") && !item_CVI.ToBsonDocument().GetValue("disponibilidad_servicio").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("disponibilidad_servicio").ToString()) ? (item_CVI.ToBsonDocument().GetValue("disponibilidad_servicio").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("disponibilidad_servicio").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("disponibilidad_servicio").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("servicio_etb") && !item_CVI.ToBsonDocument().GetValue("servicio_etb").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("servicio_etb").ToString()) ? (item_CVI.ToBsonDocument().GetValue("servicio_etb").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("servicio_etb").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("servicio_etb").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("cuenta_facturacion") && !item_CVI.ToBsonDocument().GetValue("cuenta_facturacion").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("cuenta_facturacion").ToString()) ? (item_CVI.ToBsonDocument().GetValue("cuenta_facturacion").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("cuenta_facturacion").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("cuenta_facturacion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("aliado_colaborador") && !item_CVI.ToBsonDocument().GetValue("aliado_colaborador").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("aliado_colaborador").ToString()) ? (item_CVI.ToBsonDocument().GetValue("aliado_colaborador").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("aliado_colaborador").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("aliado_colaborador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("proveedor_ultima_milla") && !item_CVI.ToBsonDocument().GetValue("proveedor_ultima_milla").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("proveedor_ultima_milla").ToString()) ? (item_CVI.ToBsonDocument().GetValue("proveedor_ultima_milla").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("proveedor_ultima_milla").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("proveedor_ultima_milla").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("medio_ultima_milla") && !item_CVI.ToBsonDocument().GetValue("medio_ultima_milla").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("medio_ultima_milla").ToString()) ? (item_CVI.ToBsonDocument().GetValue("medio_ultima_milla").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("medio_ultima_milla").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("medio_ultima_milla").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("external_service_id") && !item_CVI.ToBsonDocument().GetValue("external_service_id").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("external_service_id").ToString()) ? (item_CVI.ToBsonDocument().GetValue("external_service_id").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("external_service_id").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("external_service_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("numero_conexion") && !item_CVI.ToBsonDocument().GetValue("numero_conexion").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("numero_conexion").ToString()) ? (item_CVI.ToBsonDocument().GetValue("numero_conexion").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("numero_conexion").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("numero_conexion").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("numero_aprovisionamiento") && !item_CVI.ToBsonDocument().GetValue("numero_aprovisionamiento").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("numero_aprovisionamiento").ToString()) ? (item_CVI.ToBsonDocument().GetValue("numero_aprovisionamiento").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("numero_aprovisionamiento").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("numero_aprovisionamiento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("numero_viabilidad") && !item_CVI.ToBsonDocument().GetValue("numero_viabilidad").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("numero_viabilidad").ToString()) ? (item_CVI.ToBsonDocument().GetValue("numero_viabilidad").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("numero_viabilidad").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("numero_viabilidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("opcion_respuesta_viabilidad") && !item_CVI.ToBsonDocument().GetValue("opcion_respuesta_viabilidad").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("opcion_respuesta_viabilidad").ToString()) ? (item_CVI.ToBsonDocument().GetValue("opcion_respuesta_viabilidad").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("opcion_respuesta_viabilidad").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("opcion_respuesta_viabilidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("fecha_inicio_facturacion") && !item_CVI.ToBsonDocument().GetValue("fecha_inicio_facturacion").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("fecha_inicio_facturacion").ToString()) ? (item_CVI.ToBsonDocument().GetValue("fecha_inicio_facturacion").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("fecha_inicio_facturacion").ToString().Substring(0, 30) : item_CVI.ToBsonDocument().GetValue("fecha_inicio_facturacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                "~|" + (item_CVI.ToBsonDocument().Contains("version") && !item_CVI.ToBsonDocument().GetValue("version").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("version").ToString()) ? (item_CVI.ToBsonDocument().GetValue("version").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("version").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("version").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                "~|" + (item_CVI.ToBsonDocument().Contains("id_Aprovisionamiento") && !item_CVI.ToBsonDocument().GetValue("id_Aprovisionamiento").IsBsonNull && !string.IsNullOrEmpty(item_CVI.ToBsonDocument().GetValue("id_Aprovisionamiento").ToString()) ? (item_CVI.ToBsonDocument().GetValue("id_Aprovisionamiento").ToString().Length > 30 ? item_CVI.ToBsonDocument().GetValue("id_Aprovisionamiento").ToString().Substring(0, 29) : item_CVI.ToBsonDocument().GetValue("id_Aprovisionamiento").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                            sTextoDescarga_cv = sTextoDescarga_cv.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                            
                                            if (item_CVI.ToBsonDocument().Contains("datos_adicionales_servicio") && !item_CVI.ToBsonDocument().GetValue("datos_adicionales_servicio").IsBsonNull && item_CVI.ToBsonDocument().GetElement("datos_adicionales_servicio").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                            {
                                                datos_adicionales_CV += Extractor_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio(id_mongo);
                                            }
                                            if (item_CVI.ToBsonDocument().Contains("configuracion_servicio") && !item_CVI.ToBsonDocument().GetValue("configuracion_servicio").IsBsonNull && item_CVI.ToBsonDocument().GetElement("configuracion_servicio").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                            {
                                                configuracion_servicio_CV += Extractor_PS_SERVICIO_CLIENTE_CV_CS(id_mongo, out valores_elementos_CV);
                                            }
                                            
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                    prcManejoErrores objError = new prcManejoErrores();
                                    objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_SERVICIO_CLIENTE_CV Id: " + id_mongo);
                                    continue;
                                }
                                // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                try
                                {
                                    Conteo_PS_SERVICIO_CLIENTE_CV++;
                                    Archivo_PS_SERVICIO_CLIENTE_CV.WriteLine(sTextoDescarga_cv);
                                    Console.WriteLine("PS_SERVICIO_CLIENTE_CV ACTUALIZADA: " + itemPS_SERVICIO_CLIENTE_CV.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_CV actializadas: " + Conteo_PS_SERVICIO_CLIENTE_CV);
                                }
                                catch (Exception ex)
                                {
                                    string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                    prcManejoErrores objError = new prcManejoErrores();
                                    objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_SERVICIO_CLIENTE_CV en mongo Id: " + id_mongo);
                                    continue;
                                }
                            }
                            catch (Exception ex)
                            {
                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                prcManejoErrores objError = new prcManejoErrores();
                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_SERVICIO_CLIENTE_CV para el procesamiento de registros de mongo Id: " + id_mongo);
                                continue;
                            }
                        }

                        if (Conteo_PS_SERVICIO_CLIENTE_CV > 0)
                        {
                            Archivo_PS_SERVICIO_CLIENTE_CV.Close();                           
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_SERVICIO_CLIENTE_CV entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_SERVICIO_CLIENTE_CV.Close();
            }
            return Conteo_PS_SERVICIO_CLIENTE_CV;
        }  //Se ejecuta con Extractor_PS_SERVICIO_CLIENTE()

        internal static int Extractor_PS_SERVICIO_CLIENTE_CV_CS(string idmongo, out int valores_elementos_CVI) 
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_SERVICIO_CLIENTE_CV_CS");
            }
            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());
            StreamWriter Archivo_PS_SERVICIO_CLIENTE_CV_CS = null;
            int Conteo_PS_SERVICIO_CLIENTE_CV_CS = 0;
            valores_elementos_CVI = 0;
            string sTextoDescarga = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();            
            string archivo_CV_CS = path + "PS_SERVICIO_CLIENTE_CV_CS_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";
            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_SERVICIO_CLIENTE_CV_CS = new StreamWriter(archivo_CV_CS, true, System.Text.Encoding.GetEncoding("iso-8859-1"));
                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_SERVICIO_CLIENTE_CV_CS = db.GetCollection<BsonDocument>("PS_SERVICIO_CLIENTE");
                FilterDefinitionBuilder<BsonDocument> builderPS_SERVICIO_CLIENTE_CV_CS = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_SERVICIO_CLIENTE_CV_CS = builderPS_SERVICIO_CLIENTE_CV_CS.Empty;
                filterPS_SERVICIO_CLIENTE_CV_CS = builderPS_SERVICIO_CLIENTE_CV_CS.Eq("_id", MongoDB.Bson.ObjectId.Parse(idmongo));
                List<BsonDocument> consulta_PS_SERVICIO_CLIENTE_CV_CS = Col_PS_SERVICIO_CLIENTE_CV_CS.Find(filterPS_SERVICIO_CLIENTE_CV_CS).ToList();
                if (consulta_PS_SERVICIO_CLIENTE_CV_CS != null && consulta_PS_SERVICIO_CLIENTE_CV_CS.Count() > 0)
                {
                    try
                    {
                        foreach (var itemPS_SERVICIO_CLIENTE in consulta_PS_SERVICIO_CLIENTE_CV_CS)
                        {
                            List<BsonValue> consulta_PS_SERVICIO_CLIENTE_CVI = itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetElement("control_versiones").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_SERVICIO_CLIENTE_CVI != null && consulta_PS_SERVICIO_CLIENTE_CVI.Count() > 0)
                            {
                                foreach (var itemPS_SERVICIO_CLIENTE_CVI in consulta_PS_SERVICIO_CLIENTE_CVI)
                                {
                                    List<BsonValue> consulta_PS_SERVICIO_CLIENTE_CS = itemPS_SERVICIO_CLIENTE_CVI.ToBsonDocument().GetElement("configuracion_servicio").Value.AsBsonArray.AsQueryable().ToList();
                                    if (consulta_PS_SERVICIO_CLIENTE_CS != null && consulta_PS_SERVICIO_CLIENTE_CS.Count() > 0)
                                    {
                                        Console.WriteLine("Registros en la coleccion de PS_SERVICIO_CLIENTE_CV_CS encontrados " + consulta_PS_SERVICIO_CLIENTE_CS.Count.ToString());
                                        foreach (BsonDocument item_Configuracion_Servicio in consulta_PS_SERVICIO_CLIENTE_CS)
                                        {
                                            id_mongo = itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString();

                                            sTextoDescarga = "";
                                            try
                                            {
                                                try
                                                {
                                                    sTextoDescarga =
                                                    (itemPS_SERVICIO_CLIENTE.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                    "~|" + (itemPS_SERVICIO_CLIENTE_CVI.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_CVI.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE_CVI.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_CVI.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_CVI.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("_id") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("_id").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : item_Configuracion_Servicio.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("fecha_creacion") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("usuario_creacion") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_creacion").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_creacion").ToString().Length > 50 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_creacion").ToString().Substring(0, 50) : item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("fecha_actualizacion") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Length > 30 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Substring(0, 30) : item_Configuracion_Servicio.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("usuario_modificacion") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("id_agrupador") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("id_agrupador").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("id_agrupador").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("id_agrupador").ToString().Length > 30 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("id_agrupador").ToString().Substring(0, 29) : item_Configuracion_Servicio.ToBsonDocument().GetValue("id_agrupador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("nombre_agrupador") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_agrupador").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_agrupador").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_agrupador").ToString().Length > 30 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_agrupador").ToString().Substring(0, 29) : item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_agrupador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("nombre_elemento") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_elemento").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_elemento").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_elemento").ToString().Length > 30 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_elemento").ToString().Substring(0, 29) : item_Configuracion_Servicio.ToBsonDocument().GetValue("nombre_elemento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("cantidad") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("cantidad").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("cantidad").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("cantidad").ToString().Length > 30 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("cantidad").ToString().Substring(0, 29) : item_Configuracion_Servicio.ToBsonDocument().GetValue("cantidad").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("tipo_elemento") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("tipo_elemento").IsBsonNull && !string.IsNullOrEmpty(item_Configuracion_Servicio.ToBsonDocument().GetValue("tipo_elemento").ToString()) ? (item_Configuracion_Servicio.ToBsonDocument().GetValue("tipo_elemento").ToString().Length > 30 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("tipo_elemento").ToString().Substring(0, 29) : item_Configuracion_Servicio.ToBsonDocument().GetValue("tipo_elemento").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                    "~|" + (item_Configuracion_Servicio.ToBsonDocument().Contains("inventario_etb") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("inventario_etb").IsBsonNull ? item_Configuracion_Servicio.ToBsonDocument().GetValue("inventario_etb").ToString().Length > 8 ? item_Configuracion_Servicio.ToBsonDocument().GetValue("inventario_etb").ToString().Substring(0, 8) : item_Configuracion_Servicio.ToBsonDocument().GetValue("inventario_etb").ToString() : "");
                                                    sTextoDescarga = sTextoDescarga.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                                    if (item_Configuracion_Servicio.ToBsonDocument().Contains("valores_elementos_configuracion") && !item_Configuracion_Servicio.ToBsonDocument().GetValue("valores_elementos_configuracion").IsBsonNull && item_Configuracion_Servicio.ToBsonDocument().GetElement("valores_elementos_configuracion").Value.AsBsonArray.AsQueryable().ToList().Count() > 0)
                                                    {
                                                        string id_coleccion = id_mongo;
                                                        string id_control_version = itemPS_SERVICIO_CLIENTE_CVI.ToBsonDocument().GetValue("_id").ToString();
                                                        string configuracion_servicio = item_Configuracion_Servicio.ToBsonDocument().GetValue("_id").ToString();
                                                        valores_elementos_CVI += Extractor_PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion(id_mongo, id_control_version, configuracion_servicio);
                                                    }
                                                    
                                                }
                                                catch (Exception ex)
                                                {
                                                    string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                    prcManejoErrores objError = new prcManejoErrores();
                                                    objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_SERVICIO_CLIENTE_CV_CS Id: " + id_mongo + "," + item_Configuracion_Servicio.ToBsonDocument().GetValue("usuario_aprobacion").ToString());
                                                    continue;
                                                }
                                                // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                                try
                                                {
                                                    if (sTextoDescarga != "")
                                                    {
                                                        Archivo_PS_SERVICIO_CLIENTE_CV_CS.WriteLine(sTextoDescarga);
                                                        Conteo_PS_SERVICIO_CLIENTE_CV_CS++;
                                                        Console.WriteLine("PS_SERVICIO_CLIENTE_CV_CS ACTUALIZADA: " + itemPS_SERVICIO_CLIENTE.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_CV_CS actializadas: " + Conteo_PS_SERVICIO_CLIENTE_CV_CS);

                                                    }
                                                }
                                                catch (Exception ex)
                                                {
                                                    string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                    prcManejoErrores objError = new prcManejoErrores();
                                                    objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_SERVICIO_CLIENTE_CV_CS en mongo Id: " + id_mongo);
                                                    continue;
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_SERVICIO_CLIENTE_CV_CS para el procesamiento de registros de mongo Id: " + id_mongo);
                                                continue;
                                            }

                                        }
                                    }
                                }
                                
                            }
                        }
                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PS_SERVICIO_CLIENTE_CV_CS 
                        if (Conteo_PS_SERVICIO_CLIENTE_CV_CS > 0)
                        {
                            Archivo_PS_SERVICIO_CLIENTE_CV_CS.Close();                            
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_SERVICIO_CLIENTE_CV_CS entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_SERVICIO_CLIENTE_CV_CS.Close();
            }
            return Conteo_PS_SERVICIO_CLIENTE_CV_CS;
        } //Se ejecuta con Extractor_PS_SERVICIO_CLIENTE_CV()

        internal static int Extractor_PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion(string idmongo, string id_control_version, string configuracion_servicio)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());
            StreamWriter Archivo_PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion = null;
            int Conteo_PS_SERVICIO_CLIENTE_CV_VE = 0;
            string sTextoDescarga_CV_VE = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();

            string archivo_CV_valores_elementos_configuracion = path + "PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion = new StreamWriter(archivo_CV_valores_elementos_configuracion, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_SERVICIO_CLIENTE_CV_VE = db.GetCollection<BsonDocument>("PS_SERVICIO_CLIENTE");
                FilterDefinitionBuilder<BsonDocument> builderPS_SERVICIO_CLIENTE_CV_VE = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_SERVICIO_CLIENTE_CV_VE = builderPS_SERVICIO_CLIENTE_CV_VE.Empty;
                filterPS_SERVICIO_CLIENTE_CV_VE = builderPS_SERVICIO_CLIENTE_CV_VE.Eq("_id", MongoDB.Bson.ObjectId.Parse(idmongo));
                List<BsonDocument> consulta_PS_SERVICIO_CLIENTE_CV_VE = Col_PS_SERVICIO_CLIENTE_CV_VE.Find(filterPS_SERVICIO_CLIENTE_CV_VE).ToList();

                if (consulta_PS_SERVICIO_CLIENTE_CV_VE != null && consulta_PS_SERVICIO_CLIENTE_CV_VE.Count() > 0)
                {
                    try
                    {
                        foreach (BsonDocument itemPS_SERVICIO_CLIENTE_CV_VE in consulta_PS_SERVICIO_CLIENTE_CV_VE)
                        {
                            id_mongo = itemPS_SERVICIO_CLIENTE_CV_VE.GetValue("_id").ToString();
                            sTextoDescarga_CV_VE = "";
                            List<BsonValue> consulta_PS_SERVICIO_CLIENTE_CV = itemPS_SERVICIO_CLIENTE_CV_VE.ToBsonDocument().GetElement("control_versiones").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_SERVICIO_CLIENTE_CV != null && consulta_PS_SERVICIO_CLIENTE_CV.Count() > 0)
                            {
                                foreach (BsonValue item_CV in consulta_PS_SERVICIO_CLIENTE_CV)
                                {
                                    var consulta_PS_SERVICIO_CLIENTE_configuracion_servicio = item_CV.ToBsonDocument().GetElement("configuracion_servicio").Value.AsBsonArray.AsQueryable().ToList();
                                    if (consulta_PS_SERVICIO_CLIENTE_configuracion_servicio != null && consulta_PS_SERVICIO_CLIENTE_configuracion_servicio.Count() > 0)
                                    {
                                        foreach (BsonValue item_configuracion_servicio in consulta_PS_SERVICIO_CLIENTE_configuracion_servicio)
                                        {
                                            try
                                            {
                                                List<BsonValue> consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion = item_configuracion_servicio.ToBsonDocument().GetElement("valores_elementos_configuracion").Value.AsBsonArray.AsQueryable().ToList();
                                                if (consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion != null && consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion.Count() > 0)
                                                {
                                                    // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion 
                                                    Console.WriteLine("Registros en la coleccion de PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion encontrados " + consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion.Count.ToString());
                                                    foreach (var item_elementos_configuracion in consulta_PS_SERVICIO_CLIENTE_valores_elementos_configuracion)
                                                    {
                                                        try
                                                        { //Extractor_PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion(id_mongo, id_control_version, configuracion_servicio);
                                                            if (idmongo == itemPS_SERVICIO_CLIENTE_CV_VE.ToBsonDocument().GetValue("_id").ToString() && id_control_version == item_CV.ToBsonDocument().GetValue("_id").ToString() && configuracion_servicio == item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString())
                                                            {
                                                                sTextoDescarga_CV_VE =
                                                                (itemPS_SERVICIO_CLIENTE_CV_VE.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_CV_VE.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE_CV_VE.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_CV_VE.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_CV_VE.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                                "~|" + (item_CV.ToBsonDocument().Contains("_id") && !item_CV.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(item_CV.ToBsonDocument().GetValue("_id").ToString()) ? (item_CV.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? item_CV.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : item_CV.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                                "~|" + (item_configuracion_servicio.ToBsonDocument().Contains("_id") && !item_configuracion_servicio.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString()) ? (item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : item_configuracion_servicio.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                                "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("_id") && !item_elementos_configuracion.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                                "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("fecha_creacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString().Substring(0, 30) : item_elementos_configuracion.ToBsonDocument().GetValue("fecha_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                                "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("usuario_creacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString().Length > 50 ? item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString().Substring(0, 50) : item_elementos_configuracion.ToBsonDocument().GetValue("usuario_creacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                                "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("fecha_actualizacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString().Substring(0, 30) : item_elementos_configuracion.ToBsonDocument().GetValue("fecha_actualizacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                                "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("usuario_modificacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString().Length > 50 ? item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString().Substring(0, 50) : item_elementos_configuracion.ToBsonDocument().GetValue("usuario_modificacion").ToString()) : "") + // VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                                "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("marca") && !item_elementos_configuracion.ToBsonDocument().GetValue("marca").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("marca").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("marca").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("marca").ToString().Substring(0, 29) : item_elementos_configuracion.ToBsonDocument().GetValue("marca").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                                "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("referencia") && !item_elementos_configuracion.ToBsonDocument().GetValue("referencia").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("referencia").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("referencia").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("referencia").ToString().Substring(0, 29) : item_elementos_configuracion.ToBsonDocument().GetValue("referencia").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                                "~|" + (item_elementos_configuracion.ToBsonDocument().Contains("valor_asignacion") && !item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").IsBsonNull && !string.IsNullOrEmpty(item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").ToString()) ? (item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").ToString().Length > 30 ? item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").ToString().Substring(0, 29) : item_elementos_configuracion.ToBsonDocument().GetValue("valor_asignacion").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                                sTextoDescarga_CV_VE = sTextoDescarga_CV_VE.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                                            }
                                                            
                                                        }
                                                        catch (Exception ex)
                                                        {
                                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                            prcManejoErrores objError = new prcManejoErrores();
                                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion Id: " + id_mongo + "," + item_elementos_configuracion.ToBsonDocument().GetValue("_id").ToString());
                                                            continue;
                                                        }
                                                        // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ACTUALIZA EL REGISTRO EN LA BANDERA Y ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                                        try
                                                        {
                                                            if (sTextoDescarga_CV_VE != "")
                                                            {
                                                                Archivo_PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion.WriteLine(sTextoDescarga_CV_VE);
                                                                Conteo_PS_SERVICIO_CLIENTE_CV_VE++;
                                                            }
                                                            Console.WriteLine("PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion ACTUALIZADA: " + itemPS_SERVICIO_CLIENTE_CV_VE.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion actializadas: " + Conteo_PS_SERVICIO_CLIENTE_CV_VE);
                                                        }
                                                        catch (Exception ex)
                                                        {
                                                            string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                            prcManejoErrores objError = new prcManejoErrores();
                                                            objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion en mongo Id: " + id_mongo);
                                                            continue;
                                                        }
                                                    }
                                                }


                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion para el procesamiento de registros de mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }
                            

                        }

                        if (Conteo_PS_SERVICIO_CLIENTE_CV_VE > 0)
                        {
                            Archivo_PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion entre el modelo de datos y de registros de mongo Id: " + id_mongo);
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
                Archivo_PS_SERVICIO_CLIENTE_CV_valores_elementos_configuracion.Close();
            }
            return Conteo_PS_SERVICIO_CLIENTE_CV_VE;
        }  //Se ejecuta con Extractor_PS_SERVICIO_CLIENTE_CV_CS()

        internal static int Extractor_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio(string idmongo)
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
                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en creacion del directorio del archivo PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio");

            }

            //Conexion a DB
            MongoClient client = new MongoClient(ConfigurationManager.ConnectionStrings["ConexionMongo"].ToString());
            IMongoDatabase db = client.GetDatabase(ConfigurationManager.AppSettings["BaseDatosMongo"].ToString());
            StreamWriter Archivo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio = null;
            int Conteo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio = 0;
            string sTextoDescarga_CV_datos_adicionales_servicio = "";
            string id_mongo = "";
            DateTime fechatemp = DateTime.Now.ToUniversalTime();
            string archivo_CV_datos_adicionales_servicio = path + "PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio_" + Convert.ToDateTime(fechatemp.ToLocalTime()).ToString("ddMMyyyy") + ".txt";

            try
            {
                // Se abren los archivos para poder escribirlos
                Archivo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio = new StreamWriter(archivo_CV_datos_adicionales_servicio, true, System.Text.Encoding.GetEncoding("iso-8859-1"));

                // FILTRO PARA LAS COLECCION
                IMongoCollection<BsonDocument> Col_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio = db.GetCollection<BsonDocument>("PS_SERVICIO_CLIENTE");
                FilterDefinitionBuilder<BsonDocument> builderPS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filterPS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio = builderPS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.Empty;
                filterPS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio = builderPS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.And(
                builderPS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.Eq("_id", MongoDB.Bson.ObjectId.Parse(idmongo)),
                builderPS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.SizeGte("control_versiones.datos_adicionales_servicio", 1));


                List<BsonDocument> consulta_PS_SERVICIO_CLIENTE_CV_datos = Col_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.Find(filterPS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio).ToList();

                if (consulta_PS_SERVICIO_CLIENTE_CV_datos != null && consulta_PS_SERVICIO_CLIENTE_CV_datos.Count() > 0)
                {
                    try
                    {
                        foreach (BsonDocument itemPS_SERVICIO_CLIENTE_CV_datos in consulta_PS_SERVICIO_CLIENTE_CV_datos)
                        {
                            id_mongo = itemPS_SERVICIO_CLIENTE_CV_datos.GetValue("_id").ToString();
                            List<BsonValue> consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales = itemPS_SERVICIO_CLIENTE_CV_datos.GetElement("control_versiones").Value.AsBsonArray.AsQueryable().ToList();
                            if (consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales != null && consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales.Count() > 0)
                            {
                                foreach (var item_CV_DAS in consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales)
                                {
                                    sTextoDescarga_CV_datos_adicionales_servicio = "";
                                    List<BsonValue> consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio = item_CV_DAS.ToBsonDocument().GetElement("datos_adicionales_servicio").Value.AsBsonArray.AsQueryable().ToList();

                                    if (consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio != null && consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.Count() > 0)
                                    {
                                        // ESCRIBIR LOS DATOS OBTENIDOS DE LAS CONSULTAS POR REGISTRO DE PS_SERVICIO_CLIENTE_datos_adicionales_servicio 
                                        Console.WriteLine("Registros en la coleccion de PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio encontrados " + consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.Count.ToString());
                                        foreach (BsonValue itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio in consulta_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio)
                                        {
                                            try
                                            {
                                                try
                                                {
                                                    sTextoDescarga_CV_datos_adicionales_servicio =
                                                    (itemPS_SERVICIO_CLIENTE_CV_datos.ToBsonDocument().Contains("_id") ? !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_CV_datos.ToBsonDocument().GetValue("_id")?.ToString()) ? (itemPS_SERVICIO_CLIENTE_CV_datos.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_CV_datos.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_CV_datos.ToBsonDocument().GetValue("_id").ToString()) : "" : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
                                                     "~|" + (item_CV_DAS.ToBsonDocument().Contains("_id") && !item_CV_DAS.ToBsonDocument().GetValue("_id").IsBsonNull && !string.IsNullOrEmpty(item_CV_DAS.ToBsonDocument().GetValue("_id").ToString()) ? (item_CV_DAS.ToBsonDocument().GetValue("_id").ToString().Length > 30 ? item_CV_DAS.ToBsonDocument().GetValue("_id").ToString().Substring(0, 29) : item_CV_DAS.ToBsonDocument().GetValue("_id").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                     "~|" + (itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().Contains("identificador") && !itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").ToString()) ? (itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("identificador").ToString()) : "") +  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                     "~|" + (itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().Contains("valor") && !itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").IsBsonNull && !string.IsNullOrEmpty(itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").ToString()) ? (itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").ToString().Length > 30 ? itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").ToString().Substring(0, 29) : itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().GetValue("valor").ToString()) : "");  //VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,   
                                                    sTextoDescarga_CV_datos_adicionales_servicio = sTextoDescarga_CV_datos_adicionales_servicio.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Replace("\v", " ").Replace("\tCL", " ");
                                                }
                                                catch (Exception ex)
                                                {
                                                    string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                    prcManejoErrores objError = new prcManejoErrores();
                                                    objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia entre la validacion y el tipo de datos de PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio Id: " + id_mongo + "," + itemPS_SERVICIO_CLIENTE_datos_adicionales_servicio.ToBsonDocument().ToString());
                                                    continue;
                                                }
                                                // TERMINA DE REALIZAR LA CADENA DEL REGISTRO Y LO INCLUYE DENTRO DEL ARCHIVO, ESCRIBE EN CONSOLA LO QUE SE ESTA CORRIENDO 
                                                try
                                                {
                                                    if (sTextoDescarga_CV_datos_adicionales_servicio != "")
                                                    {
                                                        Archivo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.WriteLine(sTextoDescarga_CV_datos_adicionales_servicio);
                                                        Console.WriteLine(sTextoDescarga_CV_datos_adicionales_servicio);
                                                        Conteo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio++;
                                                    }
                                                    //Console.WriteLine("PS_SERVICIO_CLIENTE_datos_adicionales_servicio ACTUALIZADA: " + itemPS_APROVISIONAMIENTO.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_datos_adicionales_servicio actializadas: " + Conteo_PS_APROVISIONAMIENTO_Comunicaciones);
                                                }
                                                catch (Exception ex)
                                                {
                                                    string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                    prcManejoErrores objError = new prcManejoErrores();
                                                    objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en la actualizacion de la bandera en PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio en mongo Id: " + id_mongo);
                                                    continue;
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                                                prcManejoErrores objError = new prcManejoErrores();
                                                objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Error en validacion de la bandera de actualizacion PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio para el procesamiento de registros de mongo Id: " + id_mongo);
                                                continue;
                                            }
                                        }
                                        Console.WriteLine("PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio ACTUALIZADA: " + itemPS_SERVICIO_CLIENTE_CV_datos.GetValue("_id").ToString() + "Numero de PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio actializadas: " + Conteo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio);
                                    }
                                }
                            }
                        }

                        if (Conteo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio > 0)
                        {
                            Archivo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        string sNombreArchivoError = "ErrorBatch_Cargue_DWH";
                        prcManejoErrores objError = new prcManejoErrores();
                        objError.ErroresGeneral(ex, sNombreArchivoError, ex.Message.ToString() + "Inconsistencia en PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio entre el modelo de datos y de registros de mongo Id: " + id_mongo);
                    }
                }
                return Conteo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio;
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
                Archivo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio.Close();
            }
            return Conteo_PS_SERVICIO_CLIENTE_CV_datos_adicionales_servicio;
        } //Se ejecuta con Extractor_PS_SERVICIO_CLIENTE()
    }
}
