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
    public class PS_VW_EXISTENCIAS_PRODUCTO : ParentDto_ID
    {
        private ObjectId _id_estado;
        private ObjectId _id_bodega;
        private ObjectId _id_ramo;
        private ObjectId _id_tipo_material;
        
        [Display(Name = "Nombre")]
        public string descripcion { get; set; }

        [Display(Name = "Código")]
        public string codigo { get; set; }

        [Display(Name = "Proveedor")]
        public string fabricante { get; set; }

        [Display(Name = "Modelo")]
        public string modelo { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Display(Name = "Ramo")]
        public string id_ramo
        {
            get { return Convert.ToString(_id_ramo); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_ramo); }
        }
        public string ramo { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Display(Name = "Tipo de material")]
        public string id_tipo_material
        {
            get { return Convert.ToString(_id_tipo_material); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_tipo_material); }
        }
        public string tipo_material { get; set; }

        [Display(Name = "Unidad de medida")]
        public string unidad_medida { get; set; }

        [Display(Name = "Existencia mínima")]
        public decimal existencia_minima { get; set; }

        [Display(Name = "Existencia máxima")]
        public decimal existencia_maxima { get; set; }

        [Display(Name = "Cantidad")]
        public double cantidad { get; set; }

        [Display(Name = "Cantidad reservada")]
        public double cantidad_reserva { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Display(Name = "Bodega")]
        public string id_bodega
        {
            get { return Convert.ToString(_id_bodega); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_bodega); }
        }
        public string bodega { get; set; }


        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Display(Name = "Estado")]
        public string id_estado
        {
            get { return Convert.ToString(_id_estado); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado); }
        }
        public string estado { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

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

        [Display(Name = "Serializable")]
        public Nullable<bool> es_serializable { get; set; }

        [Display(Name = "Fungible")]
        public Nullable<bool> es_fungible
        {
            get
            {
                return !(this.es_serializable != null && (bool)this.es_serializable);
            }
        }

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

        [Display(Name = "Atributos")]
        public List<PS_ATRIBUTO_PRODUCTO> atributos_producto { get; set; }

    }
}
