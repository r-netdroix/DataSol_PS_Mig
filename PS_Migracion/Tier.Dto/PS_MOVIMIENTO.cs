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
    public class PS_MOVIMIENTO : ParentDto_ID_Auditoria
    {

        private ObjectId _id_inventario;
        private ObjectId _id_movimiento;
        private ObjectId _id_estado_inicial;
        private ObjectId _id_estado_final;
        private ObjectId _id_bodega_inicial;
        private ObjectId _id_bodega_final;

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
        [Display(Name = "Movimiento")]
        public string id_movimiento
        {
            get { return Convert.ToString(_id_movimiento); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_movimiento); }
        }

        public string movimiento { get; set; }

        [Display(Name = "Cantidad")]
        public double cantidad { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Estado Inicial")]
        public string id_estado_inicial
        {
            get { return Convert.ToString(_id_estado_inicial); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado_inicial); }
        }

        public string estado_inicial { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Estado Final")]
        public string id_estado_final
        {
            get { return Convert.ToString(_id_estado_final); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado_final); }
        }

        public string estado_final { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Bodega Inicial")]
        public string id_bodega_inicial
        {
            get { return Convert.ToString(_id_bodega_inicial); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_bodega_inicial); }
        }

        public string bodega_inicial { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Bodega Final")]
        public string id_bodega_final
        {
            get { return Convert.ToString(_id_bodega_final); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_bodega_final); }
        }

        public string bodega_final { get; set; }

        [Display(Name = "Observación")]
        public string observacion { get; set; }

        [Display(Name = "Fecha de Movimiento")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_movimiento { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }
    }
}
