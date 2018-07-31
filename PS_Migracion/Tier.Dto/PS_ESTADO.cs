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
    public class PS_ESTADO : ParentDto_ID_Auditoria
    {
        [Remote("ValidarNombreEstado", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        [Display(Name = "Nombre del Estado")]
        public string nombre_estado { get; set; }

        [Display(Name = "Identificador reloj")]
        [Required(ErrorMessage = "Dato requerido")]
        public string id_reloj { get; set; }

        [Display(Name = "Nombre cronómetro")]
        public string reloj { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Es inicio tarea")]
        public Nullable<bool> es_inicial_tarea { get; set; }

        [Display(Name = "Es final tarea")]
        public Nullable<bool> es_final_tarea { get; set; }
    }
}
