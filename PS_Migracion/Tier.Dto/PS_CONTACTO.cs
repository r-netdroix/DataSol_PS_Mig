using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_CONTACTO
    {
        public Nullable<Enumeradores.TipoContacto> tipo_contacto { get; set; }
        public string nombres { get; set; }
        public string apellidos { get; set; }
        public string telefono { get; set; }
        public string movil { get; set; }
        public string correo { get; set; }
    }
}