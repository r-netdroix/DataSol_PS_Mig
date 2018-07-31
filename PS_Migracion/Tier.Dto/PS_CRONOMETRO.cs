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
    public class PS_CRONOMETRO : ParentDto_ID_Auditoria
    {
        [Remote("ValidarNombreCronometro", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        [Display(Name = "Nombre del cronómetro")]
        public string nombre { get; set; }

        [Display(Name = "Descripción")]
        public string descripcion { get; set; }

        [Display(Name = "Estado")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Color del cronómetro")]
        [Required(ErrorMessage = "Dato requerido")]
        public string color { get; set; }
    }
}
