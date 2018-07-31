using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_DETALLE_OPCION_RESPUESTA_VIABILIDAD : PS_VALOR_CAMPO_DINAMICO
    {
        public byte fila_registro { get; set; }
    }
}
