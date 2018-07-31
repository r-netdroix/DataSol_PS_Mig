using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_ADJUNTO : ParentDto_ID
    {
        public string ruta { get; set; }

        [BsonElement("fecha_creacion")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> FechaCreacion { get; set; }

        [BsonElement("usuario_creacion")]
        public string UsuarioCreacion { get; set; }

        private Nullable<ObjectId> _id_tarea_relacionada;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_tarea_relacionada
        {
            get { return _id_tarea_relacionada != null ? Convert.ToString(_id_tarea_relacionada) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_tarea_relacionada = convertedID;
                }
                else
                {
                    _id_tarea_relacionada = new Nullable<ObjectId>();
                }
            }
        }

        public string tarea_relacionada { get; set; }

        [Display(Name = "Público")]
        public Nullable<bool> es_publico { get; set; }

        [Display(Name = "Privado")]
        public Nullable<bool> es_privado
        {
            get
            {
                return !(this.es_publico != null && (bool)this.es_publico);
            }
        }
    }
}