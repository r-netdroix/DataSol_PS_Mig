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
    public class PS_VW_MOVIMIENTO_INVENTARIO : PS_INVENTARIO
    {

        [BsonElement("_producto")]
        public List<PS_PRODUCTO> _producto { get; set; }

        [BsonElement("movimiento")]
        public List<PS_MOVIMIENTO> movimiento { get; set; }

        public string codigo
        {
            get
            {
                string _code = string.Empty;
                if (_producto != null && _producto.Count() > 0)
                {
                    _code = _producto.FirstOrDefault().codigo;
                }
                return _code;
            }
        }

        public string id_movimiento
        {
            get
            {
                string _id = string.Empty;
                if (movimiento != null && movimiento.Count > 0)
                {
                    _id = movimiento.FirstOrDefault().id_movimiento;
                }
                return _id;
            }
        }

        public string tipo_movimiento
        {
            get
            {
                string _movimiento = string.Empty;
                if (movimiento != null && movimiento.Count > 0)
                {
                    _movimiento = movimiento.FirstOrDefault().movimiento;
                }
                return _movimiento;
            }
        }

        [BsonIgnore]
        public DateTime? fecha_inicial { get; set; }

        [BsonIgnore]
        public DateTime? fecha_final { get; set; }


        public PS_VW_MOVIMIENTO_INVENTARIO()
        {
            this.movimiento = new List<PS_MOVIMIENTO>();
            this._producto = new List<PS_PRODUCTO>();
        }
    }
}
