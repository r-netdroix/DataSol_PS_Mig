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
    public class PS_ELEMENTO_CONFIGURACION_VALOR : PS_ELEMENTO_CONFIGURACION
    {
        [Display(Name = "Valor Elementos Configuiración")]
        public IList<PS_VALOR_ELEMENTO_CONFIGURACION> valores_elementos_configuracion { get; set; }
    }
}
