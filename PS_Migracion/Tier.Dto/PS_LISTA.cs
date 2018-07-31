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
    public class PS_LISTA : ParentDto_ID_Auditoria
    {
        [Display(Name = "Nombre")]
        [Required(ErrorMessage = "Nombre de la lista requerida")]
        public string nombre { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        private ObjectId _id_lista_dependiente;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Lista dependiente")]
        public string id_lista_dependiente
        {
            get { return Convert.ToString(_id_lista_dependiente); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_lista_dependiente); }
        }

        [Display(Name = "Elementos de la lista")]
        public List<PS_ELEMENTOS_LISTA> elementos_lista { get; set; }

        [Display(Name = "Descripción")]
        public string descripcion { get; set; }

        [Display(Name = "Módulo")]
        [Required(ErrorMessage = "Módulo de la lista requerida")]
        public string modulo { get; set; }
    }
}
