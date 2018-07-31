using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto.Inventario
{
    [BsonIgnoreExtraElements]
    public class PS_FAMILIA : ParentDto_ID_Auditoria
    {
        [Display(Name = "Identificador de la Familia")]
        public string id_familia { get; set; }

        [Display(Name = "Nombre de la Familia")]
        public string nombre_familia { get; set; }

        [Display(Name = "¿Está activa?")]
        public bool es_activo { get; set; }
    }
}
