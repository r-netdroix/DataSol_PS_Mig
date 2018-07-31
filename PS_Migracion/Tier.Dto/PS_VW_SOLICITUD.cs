using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_VW_SOLICITUD : PS_VIABILIDAD
    {
        [BsonElement("aprovisionamiento")]
        [BsonIgnoreIfNull]
        public IList<PS_APROVISIONAMIENTO> aprovisionamientos { get; set; }
    }
}
