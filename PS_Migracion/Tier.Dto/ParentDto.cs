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
    public class ParentDto_ID
    {
        private ObjectId _id;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string Id
        {
            get { return Convert.ToString(_id); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id); }
        }

    }

    [BsonIgnoreExtraElements]
    public class ParentDto_Auditoria
    {
        [BsonElement("fecha_creacion")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> FechaCreacion { get; set; }

        [BsonElement("usuario_creacion")]
        public string UsuarioCreacion { get; set; }

        [BsonElement("fecha_actualizacion")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> FechaActualizacion { get; set; }

        [BsonElement("usuario_modificacion")]
        public string UsuarioModificacion { get; set; }
    }

    [BsonIgnoreExtraElements]
    public class ParentDto_ID_Auditoria : ParentDto_ID
    {
        [BsonElement("fecha_creacion")]
        [Display(Name = "Fecha creación")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> FechaCreacion { get; set; }

        [BsonElement("usuario_creacion")]
        [Display(Name = "Usuario creación")]
        public string UsuarioCreacion { get; set; }

        [BsonElement("fecha_actualizacion")]
        [Display(Name = "Fecha actualización")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> FechaActualizacion { get; set; }

        [BsonElement("usuario_modificacion")]
        [Display(Name = "Usuario actualización")]
        public string UsuarioModificacion { get; set; }
    }
}
