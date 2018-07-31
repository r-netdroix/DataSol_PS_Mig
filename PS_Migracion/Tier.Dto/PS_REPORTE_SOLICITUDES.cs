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
    public class PS_REPORTE_SOLICITUDES : ParentDto_ID_Auditoria
    {
        public string id_solicitud { get; set; }
        public string tipo_solicitud { get; set; }
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_creacion { get; set; }
        public string tiempo_registro { get; set; }
        public string cuenta_cliente { get; set; }
        public string nit { get; set; }
        public string nombre_producto { get; set; }
        public string nombre_operacion_comercial { get; set; }
        public string tipo_operacion_plan { get; set; }
        public string id_usuario_asignado { get; set; }
        public string usuario_asignado { get; set; }
        public string id_grupo { get; set; }
        public string grupo_asignado { get; set; }
        public string id_fase { get; set; }
        public string fase_actual { get; set; }
        public string id_estado { get; set; }
        public string estado { get; set; }
        public string tiempo_cierre { get; set; }
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_respuesta { get; set; }
        public Dto.PS_RESPUESTA_VIABILIDAD respuesta { get; set; }
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_inicio_facturacion { get; set; }
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_fin_contrato { get; set; }
    }
}
