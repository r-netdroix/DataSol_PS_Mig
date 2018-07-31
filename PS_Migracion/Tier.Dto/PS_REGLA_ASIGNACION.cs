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
    public class PS_REGLA_ASIGNACION : ParentDto_ID_Auditoria
    {
        [Remote("ValidarNombreRegla", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        [Display(Name = "Nombre Regla")]
        public string nombre_regla { get; set; }

        [Display(Name = "Descripción")]
        public string descripcion { get; set; }

        public IList<Dto.PS_VALIDACION_PARAMETRO> validaciones_parametros { get; set; }

        private ObjectId _id_grupo_asignado;

        [Display(Name = "Grupo Asignado")]
        [Required(ErrorMessage = "Dato requerido")]
        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_grupo_asigando
        {
            get { return Convert.ToString(_id_grupo_asignado); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_grupo_asignado); }
        }

        [Display(Name = "Grupo Asignado")]
        public string grupo_asignado { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }
    }
}
