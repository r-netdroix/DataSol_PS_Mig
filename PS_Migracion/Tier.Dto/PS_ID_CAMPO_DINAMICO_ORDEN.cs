using MongoDB.Bson.Serialization.Attributes;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_ID_CAMPO_DINAMICO_ORDEN
    {
        public string id_campoDinamico { get; set; }
        public byte orden { get; set; }
    }
}