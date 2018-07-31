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
    public class PS_ATRIBUTO_PRODUCTO : ParentDto_ID
    {

        [Display(Name = "Nombre atributo")]
        public string nombre { get; set; }

        [Display(Name = "Atributo SAP")]
        public string atributo_sap { get; set; }

        [Display(Name = "Descripción")]
        public string descripcion { get; set; }

        [Display(Name = "Tipo de dato")]
        public string tipo_dato { get; set; }

        [Display(Name = "Valor atributo")]
        public string valor { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

    }
}
