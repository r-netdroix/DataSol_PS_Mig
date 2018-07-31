using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    /// <summary>
    /// MODELO INTERMEDIO DEL INVENTARIO (PRODUCTO E INVENTARIO)
    /// </summary>
    public class INT_INVENTARIO : ParentDto_ID_Auditoria
    {
        #region Producto
        public string id_producto { get; set; }
        public string descripcion { get; set; }
        public string fabricante { get; set; }
        public string modelo { get; set; }
        public string ramo { get; set; }
        public string tipo_material { get; set; }
        public Nullable<bool> es_activo { get; set; }
        public Nullable<bool> es_fisico { get; set; }
        public Nullable<bool> es_serializable { get; set; }
        public IList<Dto.PS_ATRIBUTO> atributos { get; set; }
        #endregion

        #region Inventario
        public string id_inventario { get; set; }
        public string serial { get; set; }
        public decimal cantidad { get; set; }
        public decimal cantidad_reserva { get; set; }
        public string estado { get; set; }
        public string bodega { get; set; }
        public string centro_costo { get; set; }
        public string ubicacion { get; set; }
        public string lote { get; set; }
        public Nullable<bool> es_activo_inv { get; set; }
        public IList<Dto.PS_ACCION_INVENTARIO> acciones { get; set; }
        #endregion
    }
}
