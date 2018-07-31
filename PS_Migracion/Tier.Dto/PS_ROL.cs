using MongoDB.Bson;
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
    public class PS_ROL : ParentDto_ID_Auditoria
    {
        [Display(Name = "Nombre")]
        [Remote("ValidarNombreRol", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        public string nombre { get; set; }

        [Display(Name = "Descripción")]
        public string descripcion { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Permisos")]
        public IList<Dto.PS_PERMISO> permisos { get; set; }

        [Display(Name = "Panel")]
        public string panel { get; set; }
    }
}
