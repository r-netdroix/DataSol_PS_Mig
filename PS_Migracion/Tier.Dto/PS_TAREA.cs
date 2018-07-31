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
    public class PS_TAREA : ParentDto_ID_Auditoria
    {
        [Remote("ValidarIdentificadorTarea", "Configuracion", ErrorMessage = "Identificador no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        [Display(Name = "Identificador de la Tarea")]
        public string id_tarea { get; set; }

        [Remote("ValidarNombreTarea", "Configuracion", ErrorMessage = "Nombre no disponible.")]
        [Required(ErrorMessage = "Dato requerido")]
        [RegularExpression("([a-zA-ZÑÁÉÍÓÚÜ 0-9]*$)", ErrorMessage = "Texto invalido, no debe contener caracteres especiales.")]
        [Display(Name = "Nombre de la Tarea")]
        public string nombre_tarea { get; set; }

        public IList<Dto.PS_VALIDACION_PARAMETRO> validaciones_parametros { get; set; }

        private Nullable<ObjectId> _id_grupo_asignacion;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Required(ErrorMessage = "Dato requerido")]
        [Key]
        [BsonElement("id_grupo_asignado")]
        public string id_grupo
        {
            get { return Convert.ToString(_id_grupo_asignacion); }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_grupo_asignacion = convertedID;
                }
                else
                {
                    _id_grupo_asignacion = new Nullable<ObjectId>();
                }
            }
        }

        [Display(Name = "Grupo Asignado")]
        public string grupo_asignado { get; set; }

        [Display(Name = "Subtareas")]
        public IList<string> tareas_predecesoras { get; set; }

        [Display(Name = "Tareas Dependientes")]
        public IList<string> tareas_dependientes { get; set; }

        [Display(Name = "Campos Dinámicos")]
        public IList<PS_ID_CAMPO_DINAMICO_ORDEN> campos_dinamicos { get; set; }

        [Display(Name = "Automatica")]
        public bool es_creacion_automatica { get; set; }

        [Display(Name = "Estado")]
        public Nullable<bool> es_activo { get; set; }

        [StringLength(500)]
        [Display(Name = "Observaciones")]
        public string observaciones { get; set; }

        [Display(Name = "Tiempo ANS (Minutos)")]
        [RegularExpression("^[0-9]*$", ErrorMessage = "El Tiempo ANS debe ser un número.")]
        [Required(ErrorMessage = "Dato requerido")]
        [Range(1, double.MaxValue, ErrorMessage = "Dato inválido, debe estar entre 1 y 65536")]
        public Nullable<double> ans { get; set; }

        private Nullable<ObjectId> _id_cambia_fase;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_cambia_fase")]
        [Display(Name = "Cambia a la fase")]
        public string id_cambia_fase
        {
            get
            {
                if (ObjectId.Empty.Equals(_id_cambia_fase) || _id_cambia_fase == null)
                {
                    return null;
                }
                else
                {
                    return Convert.ToString(_id_cambia_fase);
                }
            }

            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_cambia_fase = convertedID;
                }
                else
                {
                    _id_cambia_fase = new Nullable<ObjectId>();
                }
            }
        }

        [Display(Name = "Cambia a la fase")]
        public string cambia_fase { get; set; }

        private Nullable<ObjectId> _id_cambia_estado;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [BsonElement("id_cambia_estado")]
        [Display(Name = "Cambia al Estado")]
        public string id_cambia_estado
        {
            get
            {
                if (_id_cambia_estado == null || ObjectId.Empty.Equals(_id_cambia_estado))
                {
                    return null;
                }
                else
                {
                    return Convert.ToString(_id_cambia_estado);
                }
            }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_cambia_estado = convertedID;
                }
                else
                {
                    _id_cambia_estado = new Nullable<ObjectId>();
                }
            }
        }

        [Display(Name = "Cambia al Estado")]
        public string cambia_estado { get; set; }

        [Display(Name = "Tipo Elementos Cfg. Servicio")]
        public string elementos_cfg_servicio { get; set; }
    }
}
