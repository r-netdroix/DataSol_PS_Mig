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
    public class PS_INVENTARIO : ParentDto_ID_Auditoria
    {
        private ObjectId _id_producto;
        private ObjectId _id_bodega;
        private ObjectId _id_estado;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Producto")]
        public string id_producto
        {
            get { return Convert.ToString(_id_producto); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_producto); }
        }
        
        public string producto { get; set; }

        [Display(Name = "Serial - Identificador")]
        public string serial { get; set; }

        [Display(Name = "Cantidad")]
        [Required(ErrorMessage = "Cantidad es un dato requerido")]
        [Range(1, int.MaxValue, ErrorMessage = "Ingrese un valor mayor o igual a {1}")]
        public double cantidad { get; set; }

        [Display(Name = "Cantidad reservada")]
        public double cantidad_reserva { get; set; }

        [Display(Name = "Lote")]
        public string lote { get; set; }

        public string estado { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Estado")]
        public string id_estado
        {
            get { return Convert.ToString(_id_estado); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado); }
        }

        [Display(Name = "Centro de Costo")]
        public string centro_costo { get; set; }

        [Display(Name = "Valor Unitario")]
        [Required(ErrorMessage = "Valor unitario es un dato requerido")]
        [Range(0, int.MaxValue, ErrorMessage = "Ingrese un valor mayor o igual a {1}")]
        public double valor_unitario { get; set; }

        public string bodega { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Bodega")]
        public string id_bodega
        {
            get { return Convert.ToString(_id_bodega); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_bodega); }
        }

        [Display(Name = "Ubicación")]
        public string ubicacion { get; set; }

        [Display(Name="No. documento SAP")]
        public string documento { get; set; }

        [Display(Name = "Posición documento SAP")]
        public string posicion { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        public List<PS_ACCION_INVENTARIO> accion_inventario { get; set; }
    }
}
