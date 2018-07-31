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
    public class PS_VIABILIDAD : ParentDto_ID_Auditoria
    {
        [Display(Name = "Numero Viabiliad")]
        public string id_viabilidad { get; set; }

        public string usuariocreacionsalesforce { get; set; }

        [Display(Name = "Tipo de Solicitud")]
        public string tipo_solicitud { get; set; }

        [Display(Name = "Tipo de Oportunidad")]
        public string tipo_oportunidad { get; set; }

        [Display(Name = "Número de la Oportunidad")]
        public string numero_oportunidad { get; set; }

        [Display(Name = "Nombre de la Oportunidad")]
        public string nombre_oportunidad { get; set; }

        [Display(Name = "Número de Caso")]
        public string numero_caso { get; set; }

        [Display(Name = "Agrupador")]
        public string indicador_agrupacion { get; set; }

        [Display(Name = "Nombre Operación Comercial")]
        public string nombre_operacion_comercial { get; set; }

        [Display(Name = "Dirección de Ventas")]
        public string segmento { get; set; }

        [Display(Name = "Cuenta Cliente")]
        public string cuenta_cliente { get; set; }

        [Display(Name = "Nit")]
        public string nit { get; set; }

        [Display(Name = "Valor del Cliente")]
        public string cliente_valor { get; set; }

        [Display(Name = "Asesor Comercial")]
        public string asesor_comercial { get; set; }

        [Display(Name = "Ejecutivo de Experiencia")]
        public string ejecutivo_experiencia { get; set; }

        [Display(Name = "Tipo de Operacion")]
        public string tipo_operacion_plan { get; set; }

        [Display(Name = "Nombre del Producto")]
        public string nombre_producto { get; set; }

        [Display(Name = "Código del Producto")]
        public string codigo_producto { get; set; }

        public IList<Dto.PS_CONTACTO> contactos { get; set; }

        [Display(Name = "Sucursal de Instalación")]
        public string sucursal_instalcion { get; set; }

        [Display(Name = "Dirección")]
        public string direccion_principal { get; set; }

        [Display(Name = "Ciudad de Instalación")]
        public string ciudad_instalacion { get; set; }

        [Display(Name = "Tipología de Venta")]
        public string tipologia_venta { get; set; }

        [Display(Name = "Comentarios")]
        public string comentarios { get; set; }

        [Display(Name = "Duracion en Meses")]
        public Nullable<int> duracion_servicio_meses { get; set; }

        [Display(Name = "Duración en Días")]
        public Nullable<double> duracion_servicio_dias { get; set; }

        [Display(Name = "Detalle del Trámite")]
        public string tipo_instalacion { get; set; }

        [Display(Name = "Viabilidad Relacionada")]
        public string viabilidad_relacionada { get; set; }

        public IList<string> servicios_adicionales { get; set; }

        public IList<PS_IDENTIFICADOR_VALOR> datos_adicionales_viabilidad { get; set; }

        public IList<string> comunicaciones { get; set; }

        private Nullable<ObjectId> _id_usuario_asignacion;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_usuario_asignado")]
        public string id_usuario_asignado
        {
            get { return _id_usuario_asignacion != null ? Convert.ToString(_id_usuario_asignacion) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_usuario_asignacion = convertedID;
                }
                else
                {
                    _id_usuario_asignacion = new Nullable<ObjectId>();
                }
            }
        }
        [Display(Name = "Usuario Asignado")]
        public string usuario_asignado { get; set; }

        private Nullable<ObjectId> _id_grupo_asignacion;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_grupo_asignado")]
        public string id_grupo
        {
            get { return _id_grupo_asignacion != null ? Convert.ToString(_id_grupo_asignacion) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_grupo_asignacion = convertedID;
                }
                else
                {
                    _id_grupo_asignacion = new Nullable<ObjectId>();
                }
            }
        }
        [Display(Name = "Grupo Asignado")]
        public string grupo_asignado { get; set; }

        public List<PS_ADJUNTO> adjuntos { get; set; }
        public List<string> historico_estados { get; set; }
        public List<PS_TIEMPO_SOLICITUD> tiempos_solicitud { get; set; }

        private Nullable<ObjectId> _id_fase;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_fase")]
        public string id_fase
        {
            get { return _id_fase != null ? Convert.ToString(_id_fase) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_fase = convertedID;
                }
                else
                {
                    _id_fase = new Nullable<ObjectId>();
                }
            }
        }
        [Display(Name = "Fase Actual")]
        public string fase_actual { get; set; }

        private Nullable<ObjectId> _id_estado;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_estado")]
        public string id_estado
        {
            get { return _id_estado != null ? Convert.ToString(_id_estado) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_estado = convertedID;
                }
                else
                {
                    _id_estado = new Nullable<ObjectId>();
                }
            }
        }
        [Display(Name = "Estado Actual")]
        public string estado { get; set; }

        [Display(Name = "Fecha de Respuesta")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_respuesta { get; set; }
        public Dto.PS_RESPUESTA_VIABILIDAD respuesta { get; set; }

        public List<Dto.PS_IDENTIFICADOR_VALOR> recursos_salesforce { get; set; }

        [Display(Name = "ANS (Horas)")]
        public Nullable<double> tiempo_ans { get; set; }

        [Display(Name = "Tipo proceso")]
        public string id_tipo_proceso { get; set; }

        [Display(Name = "Tipo proceso")]
        public string tipo_proceso { get; set; }

        [Display(Name = "Visible")]
        public Nullable<bool> es_visible { get; set; }

        [Display(Name = "Fecha de Finalización")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_finalizacion { get; set; }
    }

}
