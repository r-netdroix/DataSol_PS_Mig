using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_PLANTILLA_COMUNICACION : ParentDto_ID_Auditoria
    {
        [Remote("ValidarNombrePlantillaComunicacion", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        [Display(Name = "Nombre de plantilla")]
        public string Plantilla { get; set; }

        [Display(Name = "Observaciones")]
        public string observaciones { get; set; }

        [Display(Name = "Plantilla")]
        public string htmlPlantilla { get; set; }

        [Display(Name = "Estado")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Tipo plantilla")]
        [Required(ErrorMessage = "Dato requerido")]
        public string id_tipo_plantilla { get; set; }

        [Display(Name = "Tipo Plantilla")]
        public string tipo_plantilla { get; set; }

        [Display(Name = "Producto")]
        [Required(ErrorMessage = "Dato requerido")]
        public string id_producto { get; set; }

        [Display(Name = "Producto")]
        public string producto { get; set; }

        [Display(Name = "Tipo operación")]
        [Required(ErrorMessage = "Dato requerido")]
        public string id_tipo_operacion { get; set; }

        [Display(Name = "Tipo operación")]
        public string tipo_operacion { get; set; }
    }
}
