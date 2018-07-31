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
    public class PS_ALERTAS_NOTIFICACIONES : ParentDto_ID_Auditoria
    {
        private Nullable<ObjectId> _id_usuario;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_usuario
        {
            get
            {
                if (ObjectId.Empty.Equals(_id_usuario) || _id_usuario == null)
                {
                    return null;
                }
                else
                {
                    return Convert.ToString(_id_usuario);
                }
            }

            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_usuario = convertedID;
                }
                else
                {
                    _id_usuario = new Nullable<ObjectId>();
                }
            }
        }

        [Display(Name = "Usuario")]
        public string usuario { get; set; }

        [Display(Name = "Descripción")]
        public string descripcion { get; set; }

        public string texto_alerta { get; set; }

        [Display(Name = "Visto")]
        public Nullable<bool> visto { get; set; }

        public Nullable<DateTime> fecha_visto { get; set; }

        public string idSolicitud { get; set; }
        public string tipo_solicitud { get; set; }
        public string idTareaSolicitud { get; set; }
    }
}
