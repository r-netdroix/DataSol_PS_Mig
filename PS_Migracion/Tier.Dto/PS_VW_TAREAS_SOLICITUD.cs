using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_VW_TAREAS_SOLICITUD : PS_TAREAS_SOLICITUD
    {
        public IList<PS_TAREA> tareas { get; set; }
    }
}
