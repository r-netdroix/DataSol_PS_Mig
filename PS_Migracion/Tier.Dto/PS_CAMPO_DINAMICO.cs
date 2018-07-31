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
    public class PS_CAMPO_DINAMICO : ParentDto_ID
    {
        [Display(Name = "Nombre Campo")]
        [Required(ErrorMessage = "Campo requerido")]
        [Remote("ValidaNombreCampoDinamico", "Base", ErrorMessage = "Nombre no disponible")]
        public string nombre { get; set; }

        [Display(Name = "Tipo Campo")]
        [Required(ErrorMessage = "Campo requerido")]
        public string tipo { get; set; }

        [Display(Name = "Respuesta")]
        public Nullable<bool> es_respuesta { get; set; }

        [Display(Name = "Activo")]
        public Nullable<bool> es_activo { get; set; }

        [Display(Name = "Cfg. Servicio")]
        public Nullable<bool> es_inventario { get; set; }

        private Nullable<ObjectId> _id_lista;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Lista")]
        public string id_lista
        {
            get { return _id_lista != null ? Convert.ToString(_id_lista) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_lista = convertedID;
                }
                else
                {
                    _id_lista = new Nullable<ObjectId>();
                }
            }
        }

        private Nullable<ObjectId> _id_campo_dependiente;

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        [Display(Name = "Campo Dependiente")]
        public string id_campo_dependiente
        {
            get { return _id_campo_dependiente != null ? Convert.ToString(_id_campo_dependiente) : null; }
            set
            {
                ObjectId convertedID;
                if (MongoDB.Bson.ObjectId.TryParse(value, out convertedID))
                {
                    _id_campo_dependiente = convertedID;
                }
                else
                {
                    _id_campo_dependiente = new Nullable<ObjectId>();
                }
            }
        }

        [Display(Name = "API Name SalesForce")]
        public string apiname { get; set; }

        [Required(ErrorMessage = "Campo requerido")]
        [Display(Name = "Sección")]
        public string agrupador { get; set; }
    }
}
