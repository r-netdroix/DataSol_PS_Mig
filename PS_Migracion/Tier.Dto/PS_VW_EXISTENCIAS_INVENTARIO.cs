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
    public class PS_VW_EXISTENCIAS_INVENTARIO : ParentDto_ID
    {
        private ObjectId _id_producto;
        private ObjectId _id_estado;
        private ObjectId _id_bodega;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_producto
        {
            get { return Convert.ToString(_id_producto); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_producto); }
        }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_estado
        {
            get { return Convert.ToString(_id_estado); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado); }
        }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_bodega
        {
            get { return Convert.ToString(_id_bodega); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_bodega); }
        }


        public string codigo { get; set; }

        public string descripcion { get; set; }

        public decimal existencia_minima { get; set; }

        public decimal existencia_maxima { get; set; }

        public string ramo { get; set; }

        public string tipo_material { get; set; }

        public string unidad_medida { get; set; }

        public string fabricante { get; set; }

        public string modelo { get; set; }

        public decimal cantidad { get; set; }

        public decimal cantidad_reserva { get; set; }

        public string estado { get; set; }

        public string bodega { get; set; }
    }
}
