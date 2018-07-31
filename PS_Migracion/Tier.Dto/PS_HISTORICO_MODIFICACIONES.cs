using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_HISTORICO_MODIFICACIONES : ParentDto_ID
    {
        [Display(Name = "Fecha Evento")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha { get; set; }

        [Display(Name = "Acción Efectuada")]
        public Dto.Enumeradores.AccionesHistoricoRegitros accion { get; set; }
        private ObjectId _id_usuario;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_usuario")]
        public string id_usuario
        {
            get { return Convert.ToString(_id_usuario); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_usuario); }
        }
        [Display(Name = "Usuario")]
        public string usuario { get; set; }

        [Display(Name = "Comentarios")]
        public string comentarios { get; set; }

        [Display(Name = "IP Origen")]
        public string ip { get; set; }

        [Display(Name = "Datos de la Actualizacion")]
        public string objeto_serializado { get; set; }
    }

}