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
    public class PS_PARAMETRO : ParentDto_ID_Auditoria
    {
        [Display(Name = "Nombre Parámetro")]
        [Required(ErrorMessage = "Dato requerido")]
        [Remote("ValidarNombreParametro", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        public string nombre { get; set; }

        [Display(Name = "Valor")]
        [Required(ErrorMessage = "Dato requerido")]
        public string valor { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Módulo")]
        [Required(ErrorMessage = "Dato requerido")]
        public string modulo { get; set; }
    }
}
