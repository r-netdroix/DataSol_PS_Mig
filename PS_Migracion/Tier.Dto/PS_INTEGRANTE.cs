using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_INTEGRANTE
    {

        private ObjectId _id_usuario;

        [Display(Name = "Código Usuario")]
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_usuario
        {
            get { return Convert.ToString(_id_usuario); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_usuario); }
        }

        [Display(Name = "Usuario")]
        [BsonElement("username")]
        public string alias_usuario { get; set; }

        [Display(Name = "Rol")]
        public string rol { get; set; }

        private ObjectId _id_rol;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_rol
        {
            get { return Convert.ToString(_id_rol); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_rol); }
        }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Lider")]
        public Nullable<bool> es_lider { get; set; }

        public string nombres { get; set; }
        public string apellidos { get; set; }

        public Nullable<bool> es_principal { get; set; }

        [BsonIgnore]
        ///Propiedad para las opciones multiples no se almacena en la BD
        public string id_grupo { get; set; }

        [BsonIgnore]
        ///Propiedad para las opciones multiples no se almacena en la BD
        public string grupo { get; set; }
    }
}