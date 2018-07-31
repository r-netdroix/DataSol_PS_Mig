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
    public class PS_VW_CAMPO_DINAMICO_SOLICITUD : ParentDto_ID
    {
        private ObjectId _id_solicitud;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_solicitud
        {
            get { return Convert.ToString(_id_solicitud); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_solicitud); }
        }

        public string tipo_solicitud { get; set; }

        public string numero_solicitud { get; set; }

        private ObjectId _id_tarea;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_tarea
        {
            get { return Convert.ToString(_id_tarea); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_tarea); }
        }

        public Dto.PS_VALOR_CAMPO_DINAMICO valores_campos_dinamicos { get; set; }

        public Nullable<DateTime> fecha_creacion { get; set; }
    }
}
