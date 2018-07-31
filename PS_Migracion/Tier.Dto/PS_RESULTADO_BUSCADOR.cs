using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_RESULTADO_BUSCADOR
    {
        public string texto_mostrar { get; set; }
        public string url_redireccion { get; set; }
    }
}
