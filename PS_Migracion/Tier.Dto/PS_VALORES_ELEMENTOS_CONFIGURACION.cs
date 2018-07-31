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
    public class PS_VALOR_ELEMENTO_CONFIGURACION : ParentDto_ID_Auditoria
    {
        private Nullable<ObjectId> _id_Inventario;
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_Inventario
        {
            get { return _id_Inventario != null ? Convert.ToString(_id_Inventario) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_Inventario = convertedID;
                }
                else
                {
                    _id_Inventario = new Nullable<ObjectId>();
                }
            }
        }

        [Display(Name = "Marca")]
        public string marca { get; set; }

        [Display(Name = "Referencia")]
        public string referencia { get; set; }

        [Display(Name = "Valor Asignación")]
        public string valor_asignacion { get; set; }

        [BsonIgnoreIfNull]
        [Display(Name = "Pendiente Aprovación")]
        public Nullable<bool> en_aprovacion { get; set; }
    }
}
