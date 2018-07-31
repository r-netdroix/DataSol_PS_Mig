using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_VALOR_CAMPO_DINAMICO : PS_CAMPO_DINAMICO
    {
        public string valor { get; set; }
        public byte order_visualizacion { get; set; }
    }
}