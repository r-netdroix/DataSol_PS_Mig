using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto.Inventario
{
    [BsonIgnoreExtraElements]
    public class PS_PRODUCTO : ParentDto_ID_Auditoria
    {
        [Display(Name = "Identificador del Producto")]
        public string id_producto { get; set; }

        [Display(Name = "Descripción del Producto")]
        public string descripcion_producto { get; set; }

        [Display(Name = "Código SAP")]
        public string codigo_sap { get; set; }

        [Display(Name = "Fabricante")]
        public string fabricante { get; set; }

        [Display(Name = "Modelo")]
        public string modelo { get; set; }

        private ObjectId _id_familia;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_familia")]
        public string id_familia
        {
            get { return Convert.ToString(_id_familia); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_familia); }
        }

        [Display(Name = "Familia")]
        public string familia { get; set; }

        private ObjectId _id_subfamilia;

        [Display(Name = "Sub Familia")]
        public string subfamilia { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_subfamilia")]
        public string id_subfamilia
        {
            get { return Convert.ToString(_id_subfamilia); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_subfamilia); }
        }

        private ObjectId _id_unidad_medida;

        [Display(Name = "Unidad de medida")]
        public string unidad_medida { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_unidad_medida")]
        public string id_unidad_medida
        {
            get { return Convert.ToString(_id_unidad_medida); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_unidad_medida); }
        }
        
        [Display(Name = "¿Es fungible?")]
        public bool es_fungible { get; set; }

        [Display(Name = "Fecha de creación")]
        public DateTime fecha_creacion { get; set; }

        [Display(Name = "Existencia mínima permitida")]
        public decimal existencia_minima { get; set; }

        [Display(Name = "Existencia máxima permitida")]
        public decimal existencia_maxima { get; set; }

        [Display(Name = "¿Está activo?")]
        public bool es_activo { get; set; }
    }
}
