using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_CONFIG_SERVICIO_PRODUCTO : ParentDto_ID_Auditoria
    {

        [Remote("ValidaProuducto", "Configuracion", ErrorMessage = "El producto ya  se encuentra registrado en otra configuración")]
        [Required(ErrorMessage = "Dato Requerido")]
        public string id_producto { get; set; }

        [Display(Name = "Producto")]
        public string producto { get; set; }

        [Display(Name = "Estado")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Observaciones")]
        public string observaciones { get; set; }

        [Display(Name = "Elementos de configuración")]
        public IList<PS_ELEMENTO_CONFIGURACION> elementos_configuracion { get; set; }
    }
}
