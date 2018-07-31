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
    public class PS_UNIDAD_MEDIDA : ParentDto_ID_Auditoria
    {

        [Display(Name = "Nombre unidad medida")]
        public string nombre { get; set; }

        [Display(Name = "Sigla")]
        public string sigla { get; set; }

        [Display(Name = "Tipo")]
        public string tipo { get; set; }

        [Display(Name = "Unidad a conversion")]
        public string id_unidad_medida { get; set; }

        [Display(Name = "Factor")]
        public decimal factor { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

    }
}
