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
    public class PS_FASE : ParentDto_ID_Auditoria
    {
        [Remote("ValidarNombreFase", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        [Display(Name = "Nombre Fase")]
        public string fase { get; set; }

        [Display(Name = "Estados de la fase")]
        public IList<Dto.PS_ESTADO_FASE> estados { get; set; }

        [Display(Name = "Estado Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Es fase inicial")]
        public Nullable<bool> es_inicial { get; set; }

        [Display(Name = "Es fase final")]
        public Nullable<bool> es_cierre { get; set; }
    }
}
