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
    public class PS_ACCION_INVENTARIO : ParentDto_ID_Auditoria
    {

        private ObjectId _id_tipo_accion;
        private ObjectId _id_estado_inicial;
        private ObjectId _id_estado_final;
        private ObjectId _id_aprovisionamiento;

        public string tipo_accion { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Tipo de Acción")]
        public string id_tipo_accion
        {
            get { return Convert.ToString(_id_tipo_accion); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_tipo_accion); }
        }

        [Display(Name = "Cantidad")]
        public double cantidad { get; set; }

        public string estado_inicial { get; set; }

        [Display(Name = "Estado Inicial")]
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_estado_inicial
        {
            get { return Convert.ToString(_id_estado_inicial); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado_inicial); }
        }

        public string estado_final { get; set; }

        [Display(Name = "Estado Final")]
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_estado_final
        {
            get { return Convert.ToString(_id_estado_final); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado_final); }
        }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Aprovisionamiento")]
        public string id_aprovisionamiento
        {
            get { return Convert.ToString(_id_aprovisionamiento); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_aprovisionamiento); }
        }

    }
}
