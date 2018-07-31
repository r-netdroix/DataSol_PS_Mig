using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_TIEMPO_SOLICITUD : PS_CRONOMETRO
    {
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fechainicio { get; set; }
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> Fechafin { get; set; }

        public Nullable<double> tiempoTranscurrido { get; set; }

        public Nullable<bool> en_ejecucion { get; set; }
    }
}