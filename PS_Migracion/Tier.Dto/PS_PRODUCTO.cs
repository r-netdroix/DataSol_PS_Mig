using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_PRODUCTO : ParentDto_ID_Auditoria
    {

        private ObjectId _id_ramo;
        private ObjectId _id_tipo_material;
        private ObjectId _id_unidad_medida;
        private ObjectId _id_agrupador;

        [Display(Name = "Nombre")]
        [Required(ErrorMessage = "Nombre de producto requerido")]
        public string descripcion { get; set; }

        [Display(Name = "Código")]
        [Required(ErrorMessage = "Código de producto requerido")]
        [RegularExpression("([A-Za-z0-9]{3}-[0-9]{5})", ErrorMessage = "Formato de código de producto no válido")]
        public string codigo { get; set; }

        [Display(Name = "Proveedor")]
        public string fabricante { get; set; }

        [Display(Name = "Modelo")]
        public string modelo { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Required(ErrorMessage = "Ramo de producto requerido")]
        [Key]
        [Display(Name = "Ramo")]
        public string id_ramo
        {
            get { return Convert.ToString(_id_ramo); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_ramo); }
        }

        public string ramo { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Tipo de material")]
        [Required(ErrorMessage = "Tipo de material requerido")]
        public string id_tipo_material
        {
            get { return Convert.ToString(_id_tipo_material); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_tipo_material); }
        }

        public string tipo_material { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Unidad de medida")]
        [Required(ErrorMessage = "Unidad de medida del producto requerido")]
        public string id_unidad_medida
        {
            get { return Convert.ToString(_id_unidad_medida); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_unidad_medida); }
        }

        [Display(Name = "Unidad de medida")]
        public string unidad_medida { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Agrupador producto")]
        [Required(ErrorMessage = "Agrupador de producto requerido")]
        public string id_agrupador
        {
            get { return Convert.ToString(_id_agrupador); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_agrupador); }
        }

        public string agrupador { get; set; }

        [Display(Name = "Observaciones")]
        public string comentarios { get; set; }

        [Display(Name = "Serializable")]
        public Nullable<bool> es_serializable { get; set; }

        [Display(Name = "Existencia mínima")]
        [Required(ErrorMessage = "Cantidad mínima requerida")]
        [Range(1, int.MaxValue, ErrorMessage = "Ingrese un valor mayor o igual a {1}")]
        public decimal existencia_minima { get; set; }

        [Display(Name = "Existencia máxima")]
        [Required(ErrorMessage = "Cantidad máxima requerida")]
        [Range(1, int.MaxValue, ErrorMessage = "Ingrese un valor mayor o igual a {1}")]
        public decimal existencia_maxima { get; set; }

        [Display(Name = "Físico")]
        public Nullable<bool> es_fisico { get; set; }

        [Display(Name = "Lógico")]
        public Nullable<bool> es_logico
        {
            get
            {
                return !(this.es_fisico != null && (bool)this.es_fisico);
            }
        }

        [Display(Name = "Fungible")]
        public Nullable<bool> es_fungible
        {
            get
            {
                return !(this.es_serializable != null && (bool)this.es_serializable);
            }
        }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Atributos")]
        public List<PS_ATRIBUTO_PRODUCTO> atributos_producto { get; set; }

        [Display(Name = "Es SAP")]
        public bool es_sap
        {
            get
            {
                bool _return = false;
                if (this.codigo != null)
                {
                    var regex = @"[0-9]{3}-[0-9]{5}";
                    var match = Regex.Match(this.codigo, regex, RegexOptions.IgnoreCase);
                    _return = match.Success;
                }
                return _return;
            }
        }

    }
}
