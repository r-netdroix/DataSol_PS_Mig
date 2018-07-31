using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_CAMPO_DINAMICO_ORDENADO : PS_CAMPO_DINAMICO
    {
        public byte orden_visualizacion { get; set; }
    }
}
