using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    public class PS_PRODUCTO_INVENTARIO : Dto.PS_PRODUCTO
    {
        public IList<Dto.PS_INVENTARIO> inventario { get; set; }
    }
}
