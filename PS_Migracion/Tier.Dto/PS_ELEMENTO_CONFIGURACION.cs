using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_ELEMENTO_CONFIGURACION : ParentDto_ID_Auditoria
    {
        [Display(Name = "Agrupador")]
        [Required(ErrorMessage = "Dato Requerido")]
        public string id_agrupador { get; set; }

        [Display(Name = "Agrupador")]
        public string nombre_agrupador { get; set; }

        [Display(Name = "Nombre Elemento")]
        public string nombre_elemento { get; set; }

        [Display(Name = "Cantidad")]
        [Required(ErrorMessage = "Dato Requerido")]
        public int cantidad { get; set; }

        [Display(Name = "Tipo de Elemento")]
        public Enumeradores.TipoElemento tipo_elemento { get; set; }

        [Display(Name = "Inventario ETB")]
        public Nullable<bool> inventario_etb { get; set; }
    }
}