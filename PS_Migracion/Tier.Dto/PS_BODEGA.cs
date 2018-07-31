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
    public class PS_BODEGA : ParentDto_ID_Auditoria
    {

        private ObjectId _id_usuario;

        [Display(Name = "Nombre de la Bodega")]
        [Required(ErrorMessage = "Nombre de bodega requerido")]
        public string nombre_bodega { get; set; }

        [Display(Name = "Ubicación de la Bodega")]
        [Required(ErrorMessage = "Ubicación de bodega requerida")]
        public string ubicacion_bodega { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Display(Name = "Responsable de la bodega")]
        public string id_usuario
        {
            get { return Convert.ToString(_id_usuario); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_usuario); }
        }

        public string responsable { get; set; }

        [Display(Name = "Nombre contratista")]
        public string contratista { get; set; }

        [Display(Name = "Bodega SAP")]
        public Nullable<bool> es_sap { get; set; }

        [Display(Name = "Bodega PS")]
        public bool es_gestor
        {
            get
            {
                return !(this.es_sap != null && (bool)this.es_sap);
            }
        }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }
    }
}
