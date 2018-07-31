using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_APROVISIONAMIENTO : ParentDto_ID_Auditoria
    {
        [Display(Name = "Tipo de Solicitud")]
        public string tipo_solicitud { get; set; }

        [Display(Name = "Número Aprovisionamiento")]
        public string id_aprovisionamiento { get; set; }

        [Display(Name = "Viabilidad Origen")]
        public string viabilidad_origen { get; set; }

        [Display(Name = "Opción Seleccionada")]
        public string opcion_viabilidad { get; set; }

        [Display(Name = "Comentarios")]
        public string comentarios { get; set; }

        [Display(Name = "Número de Contrato")]
        public string numero_contrato { get; set; }

        [Display(Name = "Inicio Contrato")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_inicio_contrato { get; set; }

        [Display(Name = "Fin Contrato")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_fin_contrato { get; set; }

        [Display(Name = "Renovación Automática")]
        public Nullable<bool> renovacion_automatica { get; set; }

        public IList<PS_IDENTIFICADOR_VALOR> datos_adicionales_aprovisionamiento { get; set; }

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

        [Display(Name = "ANS (Horas)")]
        public Nullable<double> tiempo_ans { get; set; }

        [Display(Name = "Tipo proceso")]
        public string id_tipo_proceso { get; set; }

        [Display(Name = "Tipo proceso")]
        public string tipo_proceso { get; set; }

        [Display(Name = "Fecha Inicio Facturación")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_inicio_facturacion { get; set; }

        [Display(Name = "Servicios Cliente")]
        public IList<string> servicios_cliente { get; set; }

        public Dto.PS_DETALLE_SOLICITUD_ENTREGA_MATERIALES solicitud_entrega_materiales { get; set; }

        [Display(Name = "Fecha de Finalización")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_finalizacion { get; set; }
    }
}

