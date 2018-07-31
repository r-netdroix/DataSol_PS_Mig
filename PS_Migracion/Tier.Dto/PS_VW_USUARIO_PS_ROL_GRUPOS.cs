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
    public class PS_VW_USUARIO_PS_ROL_GRUPOS : PS_USUARIO
    {
        [Display(Name = "Grupos Asociados")]
        public IList<string> grupos { get; set; }
    }
}
