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
    public class PS_COMUNICACION : ParentDto_ID_Auditoria
    {
        [Display(Name = "Destinatarios")]
        [Required(ErrorMessage = "Dato Requerido")]
        public IList<string> destinatarios { get; set; }

        [Display(Name = "Asunto:")]
        [Required(ErrorMessage = "Dato Requerido")]
        public string asunto { get; set; }

        [Display(Name = "Cuepo Mensaje")]
        [Required(ErrorMessage = "Dato Requerido")]
        public string mensaje_html { get; set; }

        [Display(Name = "Archivos Asjuntos")]
        public IList<string> adjuntos_comunicaciones { get; set; }

        private Nullable<ObjectId> _id_tarea_relacionada;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_tarea_relacionada
        {
            get { return _id_tarea_relacionada != null ? Convert.ToString(_id_tarea_relacionada) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_tarea_relacionada = convertedID;
                }
                else
                {
                    _id_tarea_relacionada = new Nullable<ObjectId>();
                }
            }
        }

        public string tarea_relacionada { get; set; }
    }
}
