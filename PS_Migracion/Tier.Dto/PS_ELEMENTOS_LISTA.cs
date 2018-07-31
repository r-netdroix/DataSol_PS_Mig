using MongoDB.Bson.Serialization.Attributes;
using System;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_ELEMENTOS_LISTA : ParentDto_ID
    {
        [Display(Name = "Valor del elemento")]
        public string valor { get; set; }

        [Display(Name = "Texto a mostrar")]
        public string texto { get; set; }

        [Display(Name = "Valor relacionado")]
        public string valor_relacion { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }
    }
}