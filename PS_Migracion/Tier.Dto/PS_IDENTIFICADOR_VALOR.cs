using MongoDB.Bson.Serialization.Attributes;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_IDENTIFICADOR_VALOR
    {
        public string identificador { get; set; }
        public string valor { get; set; }
    }
}