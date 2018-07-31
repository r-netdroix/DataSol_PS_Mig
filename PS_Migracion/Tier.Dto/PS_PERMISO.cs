using MongoDB.Bson.Serialization.Attributes;
using System;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_PERMISO
    {
        [Display(Name = "Funcionalidad")]
        public string funcionalidad { get; set; }

        [Display(Name = "Accion")]
        public string accion { get; set; }

        [Display(Name = "Permitido")]
        public Nullable<Boolean> permitido { get; set; }

        [Display(Name = "Elementos")]
        public Nullable<Boolean> todos_elementos { get; set; }
    }
}