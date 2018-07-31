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
    public class PS_CONSECUTIVO : ParentDto_ID_Auditoria
    {
        [Display(Name = "Nombre entidad")]
        public string entidad { get; set; }

        [Display(Name = "Valor actual")]
        public Int64 valor { get; set; }

        [Display(Name = "Formato de serie")]
        public string formato { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }
    }
}
