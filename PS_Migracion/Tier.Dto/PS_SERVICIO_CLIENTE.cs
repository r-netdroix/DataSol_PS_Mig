using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_SERVICIO_CLIENTE : ParentDto_ID_Auditoria
    {
        [Display(Name = "Estado")]
        public string estado { get; set; }

        [Display(Name = "Ancho Banda")]
        public string ancho_banda { get; set; }

        [Display(Name = "Cuenta Cliente")]
        public string cuenta_cliente { get; set; }

        [Display(Name = "Nit")]
        public string nit { get; set; }

        [Display(Name = "Código Servicio")]
        public string id_servicio { get; set; }

        [Display(Name = "Nombre Producto")]
        public string nombre_producto { get; set; }

        [Display(Name = "Plan")]
        public string plan { get; set; }

        [Display(Name = "Sucursal")]
        public string sucursal { get; set; }

        [Display(Name = "Ciudad")]
        public string ciudad { get; set; }

        [Display(Name = "Disponibilidad Servicio")]
        public string disponibilidad_servicio { get; set; }

        [Display(Name = "Servicio Etb")]
        public string servicio_etb { get; set; }

        [Display(Name = "Cuenta Facturacion")]
        public string cuenta_facturacion { get; set; }

        [Display(Name = "Aliado Colaborador")]
        public string aliado_colaborador { get; set; }

        [Display(Name = "Proveedor Última Milla")]
        public string proveedor_ultima_milla { get; set; }

        [Display(Name = "Medio Última Milla")]
        public string medio_ultima_milla { get; set; }

        [Display(Name = "External Service ID")]
        public string external_service_id { get; set; }

        [Display(Name = "Número Conexión")]
        public string numero_conexion { get; set; }

        [Display(Name = "Número Aprovisionamiento")]
        public string numero_aprovisionamiento { get; set; }

        [Display(Name = "Número Viabilidad")]
        public string numero_viabilidad { get; set; }

        [Display(Name = "Opción Respuesta Viabilidad")]
        public string opcion_respuesta_viabilidad { get; set; }

        [Display(Name = "Fecha Inicio Facturación")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_inicio_facturacion { get; set; }

        [Display(Name = "Versión")]
        public Nullable<short> version { get; set; }

        private Nullable<ObjectId> _id_Aprovisionamiento;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_Aprovisionamiento")]
        public string id_Aprovisionamiento
        {
            get { return _id_Aprovisionamiento != null ? Convert.ToString(_id_Aprovisionamiento) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_Aprovisionamiento = convertedID;
                }
                else
                {
                    _id_Aprovisionamiento = new Nullable<ObjectId>();
                }
            }
        }

        [Display(Name = "Datos Adicionales Servicio")]
        public IList<PS_IDENTIFICADOR_VALOR> datos_adicionales_servicio { get; set; }

        [Display(Name = "Configuración Servicio")]
        public IList<PS_ELEMENTO_CONFIGURACION_VALOR> configuracion_servicio { get; set; }

        [Display(Name = "Histórico modificaciones")]
        public IList<string> historico_modificaciones { get; set; }

        [Display(Name = "Control Versiones")]
        [BsonIgnoreIfNull]
        public IList<PS_SERVICIO_CLIENTE> control_versiones { get; set; }

    }
}
