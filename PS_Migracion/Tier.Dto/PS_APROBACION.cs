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
    public class PS_APROBACION : ParentDto_ID_Auditoria
    {

        private ObjectId _id_movimiento;
        private ObjectId _id_estado;
        private ObjectId _id_tipo_aprobacion;
        private ObjectId _id_aprovisionamiento;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Movimiento")]
        public string id_movimiento
        {
            get { return Convert.ToString(_id_movimiento); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_movimiento); }
        }

        public string movimiento { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Estado")]
        public string id_estado
        {
            get { return Convert.ToString(_id_estado); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado); }
        }

        public string estado { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Aprovisionamiento")]
        public string id_aprovisionamiento
        {
            get { return Convert.ToString(_id_aprovisionamiento); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_aprovisionamiento); }
        }

        public string numero_orden { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Tipo Aprobación")]
        public string id_tipo_aprobacion
        {
            get { return Convert.ToString(_id_tipo_aprobacion); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_tipo_aprobacion); }
        }

        public string tipo_aprobacion { get; set; }

        [Display(Name = "Fecha de Solicitud")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_solicitud { get; set; }

        [Display(Name = "Usuario de la Solicitud")]
        public string usuario_solicitud { get; set; }

        [Display(Name = "Observación")]
        public string observacion { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Código Verificación SMS")]
        public string codigo_verificacion { get; set; }

        public List<PS_USUARIOS_APROBACION> usuario_aprobacion { get; set; }

        public List<ITEMS_INVENTARIO> items_inventario { get; set; }
    }

    [BsonIgnoreExtraElements]
    public class PS_USUARIOS_APROBACION
    {
        [Display(Name = "Usuario de Aprobación")]
        public string usuario_aprobacion { get; set; }

        [Display(Name = "Fecha de Aprobación")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_aprobacion { get; set; }

        [Display(Name = "Rol de Usuario")]
        public string rol_aprobacion { get; set; }

        [Display(Name = "Aprobado")]
        public Nullable<bool> es_aprobado { get; set; }

        [Display(Name = "Firma usuario")]
        public string firma { get; set; }

    }

    [BsonIgnoreExtraElements]
    public class ITEMS_INVENTARIO
    {
        public string id_inventario { get; set; }

        public string producto_inventario { get; set; }

        public double cantidad { get; set; }

        public string serial { get; set; }
    }
}
