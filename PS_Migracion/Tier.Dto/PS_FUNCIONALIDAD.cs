using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_FUNCIONALIDAD : ParentDto_ID
    {
        [Display(Name = "Funcionalidad")]
        public string nombre_funcionalidad { get; set; }

        [Display(Name = "Acciones")]
        public IList<string> acciones { get; set; }

        [Display(Name = "Estado")]
        public Nullable<bool> es_activo { get; set; }
    }
}
