using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    public class PS_DETALLE_SOLICITUD_ENTREGA_MATERIALES : ParentDto_Auditoria
    {
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_solicitud { get; set; }

        public string id_empresa { get; set; }

        public string empresa { get; set; }

        public string nombre_receptor { get; set; }

        public string apellidos_receptor { get; set; }

        public string identificacion_receptor { get; set; }

        public IList<Dto.PS_USUARIO> usuarios_aprovadores { get; set; }
    }
}
