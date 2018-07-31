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
    [Serializable]
    public class PS_TAREAS_SOLICITUD : ParentDto_ID_Auditoria
    {
        private Nullable<ObjectId> _id_solicitud;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_solicitud
        {
            get { return Convert.ToString(_id_solicitud); }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_solicitud = convertedID;
                }
                else
                {
                    _id_solicitud = new Nullable<ObjectId>();
                }
            }
        }

        [Display(Name = "Tipo de Solicitud")]
        public string tipo_solicitud { get; set; }
        [Display(Name = "Número de Solicitud")]
        public string numero_solicitud { get; set; }

        private Nullable<ObjectId> _id_tarea;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_tarea
        {
            get { return Convert.ToString(_id_tarea); }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_tarea = convertedID;
                }
                else
                {
                    _id_tarea = new Nullable<ObjectId>();
                }
            }
        }

        public IList<PS_VALOR_CAMPO_DINAMICO> valores_campos_dinamicos { get; set; }

        private Nullable<ObjectId> _id_grupo_asignacion;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_grupo_asignado
        {
            get { return Convert.ToString(_id_grupo_asignacion); }
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

        private Nullable<ObjectId> _id_usuario_asignado;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_usuario_asignado
        {
            get { return Convert.ToString(_id_usuario_asignado); }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_usuario_asignado = convertedID;
                }
                else
                {
                    _id_usuario_asignado = new Nullable<ObjectId>();
                }
            }
        }
        [Display(Name = "Usuario Asignado")]
        public string usuario_asignado { get; set; }

        public IList<PS_ADJUNTO> adjuntos { get; set; }
        public IList<string> historico_modificaciones { get; set; }

        [Display(Name = "Estado")]
        public string estado { get; set; }

        private Nullable<ObjectId> _id_estado;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_estado
        {
            get { return Convert.ToString(_id_estado); }
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

        [Display(Name = "Fecha de Asignación")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_asignacion { get; set; }

        [Display(Name = "Observaciones")]
        public string observaciones { get; set; }

        public IList<string> comunicaciones { get; set; }

        [Display(Name = "Fecha de Finalización")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_finalizacion { get; set; }
    }
}
