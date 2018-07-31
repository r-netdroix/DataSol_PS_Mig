using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_RESPUESTA_VIABILIDAD : ParentDto_Auditoria
    {
        [Display(Name = "Respuesta")]
        [Required(ErrorMessage = "Dato requerido")]
        public string id_respuesta_viabilidad { get; set; }
        [Display(Name = "Respuesta")]
        public string respuesta_viabilidad { get; set; }

        [Display(Name = "Causal")]
        [Required(ErrorMessage = "Dato requerido")]
        public string id_causal_respuesta_viabilidad { get; set; }
        [Display(Name = "Causal")]
        public string causal_respuesta_viabilidad { get; set; }

        [Display(Name = "Respuesta Comercial")]
        public string respuesta_comercial { get; set; }

        public IList<PS_OPCION_RESPUESTA_VIABILIDAD> opciones { get; set; }
    }
}