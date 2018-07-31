using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_GRUPO_ASIGNACION : ParentDto_ID_Auditoria
    {
        [Remote("ValidarNombreGrupo", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        [Display(Name = "Nombre")]
        [Required(ErrorMessage = "Dato requerido")]
        public string nombre { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Predeterminado Viabilidad")]
        public Nullable<bool> es_predeterminado_viabilidad { get; set; }

        [Display(Name = "Predeterminado Aprovisionamiento")]
        public Nullable<bool> es_predeterminado_aprovisionamiento { get; set; }

        [Display(Name = "Integrantes")]
        public IList<PS_INTEGRANTE> integrantes { get; set; }

        [Display(Name = "Direccion")]
        public string direccion { get; set; }

        [Display(Name = "Gerencia")]
        public string gerencia { get; set; }

        [Display(Name = "Tipo Proceso")]
        public string tipo_proceso { get; set; }

        [Display(Name = "Descripcion")]
        public string descripcion { get; set; }

    }
}
