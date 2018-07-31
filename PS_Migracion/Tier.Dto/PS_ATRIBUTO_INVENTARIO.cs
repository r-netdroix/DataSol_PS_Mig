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
    public class PS_ATRIBUTO_INVENTARIO : ParentDto_ID_Auditoria
    {

        private ObjectId _id_inventario;
        private ObjectId _id_atributo;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Inventario")]
        public string id_inventario
        {
            get { return Convert.ToString(_id_inventario); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_inventario); }
        }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Atributo")]
        public string id_atributo
        {
            get { return Convert.ToString(_id_atributo); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_atributo); }
        }

        public string atributo { get; set; }

        [Display(Name = "Valor del Atributo")]
        public string valor_atributo { get; set; }

        [Display(Name = "¿Activo?")]
        public Nullable<bool> es_activo { get; set; }
    }
}
