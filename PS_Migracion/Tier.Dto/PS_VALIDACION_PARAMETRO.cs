using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_VALIDACION_PARAMETRO
    {
        public string campo_validacion { get; set; }

        public string parametro_validacion { get; set; }

        public IList<string> valor_validacion { get; set; }
    }
}
