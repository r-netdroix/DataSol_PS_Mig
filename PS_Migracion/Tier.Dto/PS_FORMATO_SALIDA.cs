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
    public class PS_FORMATO_SALIDA : ParentDto_ID_Auditoria
    {

        private ObjectId _id_bodega;
        private ObjectId _id_aprovisionamiento;

        [Display(Name = "Fecha entrega")]
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public DateTime fecha_entrega { get; set; }

        [BsonRepresentation(BsonType.ObjectId)]
        [Display(Name = "Destino")]
        public string id_bodega
        {
            get { return Convert.ToString(_id_bodega); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_bodega); }
        }

        public string bodega { get; set; }

        [Display(Name = "Persona(s) que solicita(n)")]
        public List<USUARIO_FORMATO_SALIDA> usuario_solicita { get; set; }

        [Display(Name = "Área consumidora")]
        public string area_consumidora { get; set; }

        [Display(Name = "No. Movimiento contable")]
        public string movimiento_contable { get; set; }

        public List<ITEMS_FORMATO_SALIDA> elementos_solicitados { get; set; }

        [Display(Name = "Persona(s) que aprueba(n)")]
        public List<USUARIO_FORMATO_SALIDA> usuario_aprueba { get; set; }

        [Display(Name = "Solicitado por")]
        public string solicitado { get; set; }

        [Display(Name = "Empresa que instala")]
        public string empresa_instala { get; set; }

        [Display(Name = "Lugar despacho")]
        public string lugar_despacho { get; set; }

        [BsonRepresentation(BsonType.ObjectId)]
        [Display(Name = "Orden asociada")]
        public string id_aprovisionamiento
        {
            get { return Convert.ToString(_id_aprovisionamiento); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_aprovisionamiento); }
        }

        [Display(Name = "Nombre cliente")]
        public string nombre_cliente { get; set; }

        [Display(Name = "Dirección cliente")]
        public string direccion_cliente { get; set; }

        [Display(Name = "Ciudad")]
        public string ciudad_cliente { get; set; }

        [Display(Name = "Teléfono cliente")]
        public string telefono_cliente { get; set; }

        [Display(Name = "Nombre contacto")]
        public string contacto_cliente { get; set; }

        [Display(Name = "Orden")]
        public string orden { get; set; }

        [Display(Name = "Consecutivo")]
        public string consecutivo { get; set; }

        [Display(Name = "Recibido")]
        public Nullable<bool> recibido { get; set; }

        [Display(Name ="Formatos generados")]
        public int impresiones { get; set; }

        [Display(Name = "Aprobada")]
        public bool aprobada
        {
            get
            {
                bool _return = false;
                if (this.usuario_aprueba != null)
                {
                    _return = true;
                    foreach (USUARIO_FORMATO_SALIDA _usr in this.usuario_aprueba)
                    {
                        _return &= !string.IsNullOrEmpty(_usr.firma);
                    }
                }
                return _return;
            }
        }

        [Display(Name = "Solicitante")]
        public string solicitante
        {
            get
            {
                string _return = string.Empty;
                if (this.usuario_solicita != null)
                {
                    List<string> _soli = new List<string>();
                    foreach (USUARIO_FORMATO_SALIDA _usr in this.usuario_solicita)
                    {
                        _soli.Add(_usr.nombre + " " + _usr.apellido);
                    }
                    _return = string.Join(" // ", _soli.ToArray());
                }
                return _return;
            }

        }

        [Display(Name = "Ids Solicitante")]
        public string id_solicitantes
        {
            get
            {
                string _return = string.Empty;
                if (this.usuario_solicita != null)
                {
                    List<string> _soli = new List<string>();
                    foreach (USUARIO_FORMATO_SALIDA _usr in this.usuario_solicita)
                    {
                        _soli.Add(_usr.identificacion);
                    }
                    _return = string.Join(" // ", _soli.ToArray());
                }
                return _return;
            }

        }

        public string aprobadores(int counter)
        {
            string _return = string.Empty;
            int count = 1;
            if (this.usuario_aprueba != null)
            {
                List<string> _apro = new List<string>();
                if (counter <= this.usuario_aprueba.Count())
                {
                    foreach (USUARIO_FORMATO_SALIDA _usr in this.usuario_aprueba)
                    {
                        if (count == counter)
                        {
                            _apro.Add(_usr.nombre + " " + _usr.apellido + " - " + _usr.identificacion);
                            break;
                        }
                        count++;
                    }
                    _return = string.Join(" // ", _apro.ToArray());
                }
            }
            return _return;
        }

        public string firmas(int counter)
        {
            string _return = string.Empty;
            int count = 1;
            if (this.usuario_aprueba != null)
            {
                if (counter <= this.usuario_aprueba.Count())
                {
                    foreach (USUARIO_FORMATO_SALIDA _usr in this.usuario_aprueba)
                    {
                        _return = _usr.firma ?? string.Empty;
                        if (count == counter)
                        {
                            break;
                        }
                        else
                        {
                            count++;
                        }
                    }
                }
            }
            return _return;
        }

        public PS_FORMATO_SALIDA()
        {
            this.impresiones = 0;
            this.elementos_solicitados = new List<ITEMS_FORMATO_SALIDA>();
            this.usuario_aprueba = new List<USUARIO_FORMATO_SALIDA>();
            this.usuario_solicita = new List<USUARIO_FORMATO_SALIDA>();
        }

        public void GenerateItemsId()
        {
            if (this.elementos_solicitados != null)
            {
                int counter = 1;
                foreach(ITEMS_FORMATO_SALIDA _item in this.elementos_solicitados)
                {
                    _item.item = counter++;
                }
            }
        }

    }

    [BsonIgnoreExtraElements]
    public class ITEMS_FORMATO_SALIDA
    {

        private ObjectId _id_inventario;
        private ObjectId _id_producto;

        [Display(Name = "Item")]
        public int item { get; set; }

        [Display(Name = "Código")]
        public string codigo { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_inventario
        {
            get { return Convert.ToString(_id_inventario); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_inventario); }
        }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_producto
        {
            get { return Convert.ToString(_id_producto); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_producto); }
        }

        [Display(Name = "Descripción")]
        public string descripcion { get; set; }

        [Display(Name = "Unidad")]
        public string unidad { get; set; }

        [Display(Name = "Cantidad")]
        public double cantidad { get; set; }
        
        [Display(Name = "Serial")]
        public string serial { get; set; }

        [Display(Name = "Ubicación")]
        public string ubicacion { get; set; }

        [Display(Name = "Reserva")]
        public string reserva { get; set; }

    }

    [BsonIgnoreExtraElements]
    public class USUARIO_FORMATO_SALIDA
    {

        private ObjectId _id_usuario;

        [Display(Name = "Rol")]
        public string rol { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        [Key]
        public string id_usuario
        {
            get { return Convert.ToString(_id_usuario); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_usuario); }
        }

        [Display(Name = "Usuario")]
        public string usuario { get; set; }

        [Display(Name = "Nombres")]
        public string nombre { get; set; }

        [Display(Name = "Apellidos")]
        public string apellido { get; set; }

        [Display(Name = "ETB")]
        public string etb { get; set; }

        [Display(Name = "Empresa")]
        public string empresa { get; set; }

        [Display(Name = "Identificación")]
        public string identificacion { get; set; }

        [Display(Name = "Correo electrónico")]
        public string correo_electronico { get; set; }

        [Display(Name = "Firma")]
        public string firma { get; set; }

    }


}
