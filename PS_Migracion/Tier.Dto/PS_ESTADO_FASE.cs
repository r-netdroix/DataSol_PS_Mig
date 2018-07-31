using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_ESTADO_FASE : ParentDto_ID
    {
        public string texto { get; set; }
        public Nullable<bool> es_inicial { get; set; }
        public Nullable<bool> es_cierre { get; set; }
    }
}