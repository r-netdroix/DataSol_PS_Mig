using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.ComponentModel.DataAnnotations;
using System.Web.Mvc;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_USUARIO : ParentDto_ID_Auditoria
    {
        [Display(Name = "Usuario")]
        [Remote("ValidarUserName", "Configuracion", ErrorMessage = "Nombre usuario de aplicación (UserName) no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        public string username { get; set; }

        private ObjectId _id_rol;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Rol")]
        [Required(ErrorMessage = "Dato requerido")]
        public string id_rol
        {
            get { return Convert.ToString(_id_rol); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_rol); }
        }
        [Display(Name = "Rol")]
        public string rol { get; set; }

        [Display(Name = "Tipo de Identificación")]
        public string id_tipo_identificacion { get; set; }

        [Display(Name = "Tipo de Identificación")]
        public string tipo_identificacion { get; set; }

        [Display(Name = "Identificación")]
        public string identificacion { get; set; }

        [Display(Name = "Nombres")]
        public string nombres { get; set; }

        [Display(Name = "Apellidos")]
        public string apellidos { get; set; }

        [Display(Name = "División")]
        public string id_division { get; set; }

        [Display(Name = "División")]
        public string division { get; set; }

        [Display(Name = "Correo Electrónico")]
        [DataType(DataType.EmailAddress, ErrorMessage ="Correo electrónico inválido.")]
        [EmailAddress(ErrorMessage = "Correo electrónico inválido.")]
        public string correo_electronico { get; set; }

        [StringLength(8, MinimumLength = 8, ErrorMessage = "La longitud del teléfono fijo es de 8 caracteres.")]
        [Display(Name = "Tel. Fijo")]
        [RegularExpression("^[0-9]*$", ErrorMessage ="Número de teléfono fijo inválido.")]
        public string telefono_fijo { get; set; }

        [StringLength(10, MinimumLength = 10, ErrorMessage = "La longitud del teléfono móvil es de 8 caracteres.")]
        [Display(Name = "Tel. Móvil")]
        [RegularExpression("^[0-9]*$", ErrorMessage ="Número de teléfono móvil inválido.")]
        public string telefono_celular { get; set; }

        [Display(Name = "Estado")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Tipo de Contrato")]
        public string id_tipo_contrato { get; set; }

        [Display(Name = "Tipo de Contrato")]
        public string tipo_contrato { get; set; }

        [Display(Name = "Empresa")]
        public string id_empresa { get; set; }

        [Display(Name = "Empresa")]
        public string empresa { get; set; }

        [Display(Name = "Grupos Lider")]
        public IList<string> grupos_lider { get; set; }

        public PS_USUARIO() {
            grupos_lider = new List<string>();
        }
    }
}

