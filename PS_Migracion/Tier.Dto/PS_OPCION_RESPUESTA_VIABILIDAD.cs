using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_OPCION_RESPUESTA_VIABILIDAD : ParentDto_ID_Auditoria
    {
        public string nombre_opcion { get; set; }

        public string observaciones { get; set; }

        public IList<PS_ELEMENTO_CONFIGURACION_VALOR> configuracion_servicio { get; set; }

        public IList<PS_DETALLE_OPCION_RESPUESTA_VIABILIDAD> detalle_opcion { get; set; }
    }
}
