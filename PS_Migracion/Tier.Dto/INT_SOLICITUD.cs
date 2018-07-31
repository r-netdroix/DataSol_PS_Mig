using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    /// <summary>
    /// MODELO INTERMEDIO DE LA SOLICITU PARA EL ACOPLE DE LAS VIABILIDADES Y APROVISIONAMIENTOS
    /// </summary>
    public class INT_SOLICITUD : ParentDto_ID_Auditoria
    {
        public string usuariocreacionsalesforce { get; set; }

        public string id_solicitud { get; set; }
        public string tipo_solicitud { get; set; }
        public string tipo_oportunidad { get; set; }
        public string numero_oportunidad { get; set; }
        public string nombre_oportunidad { get; set; }
        public string numero_caso { get; set; }
        public string indicador_agrupacion { get; set; }
        public string nombre_operacion_comercial { get; set; }
        public string segmento { get; set; }
        public string cuenta_cliente { get; set; }
        public string nit { get; set; }
        public string cliente_valor { get; set; }
        public string asesor_comercial { get; set; }
        public string ejecutivo_experiencia { get; set; }
        public string tipo_operacion_plan { get; set; }

        public string nombre_producto { get; set; }
        public string codigo_producto { get; set; }

        public IList<Dto.PS_CONTACTO> contactos { get; set; }

        public string sucursal_instalacion { get; set; }
        public string direccion_principal { get; set; }
        public string ciudad_instalacion { get; set; }
        public string tipologia_venta { get; set; }
        public string comentarios { get; set; }

        public Nullable<int> duracion_servicio_meses { get; set; }
        public Nullable<double> duracion_servicio_dias { get; set; }
        public string tipo_instalacion { get; set; }

        public IList<string> servicios_adicionales { get; set; }
        public IList<PS_IDENTIFICADOR_VALOR> datos_adicionales_viabilidad { get; set; }
        public IList<PS_IDENTIFICADOR_VALOR> datos_adicionales_aprovisionamiento { get; set; }
        public IList<string> comunicaciones { get; set; }


        #region Propiedades Aprovisionamiento
        public string numero_contrato { get; set; }
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_inicio_contrato { get; set; }
        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public Nullable<DateTime> fecha_fin_contrato { get; set; }
        public Nullable<bool> renovacion_automatica { get; set; }

        public List<PS_ADJUNTO> adjuntos { get; set; }
        public List<string> historico_estados { get; set; }
        public List<PS_TIEMPO_SOLICITUD> tiempos_solicitud { get; set; }
        #endregion


        public string id_grupo { get; set; }
        public string grupo_asignado { get; set; }
        public string id_usuario { get; set; }
        public string usuario_asignado { get; set; }
        public string id_fase { get; set; }
        public string fase_actual { get; set; }
        public string id_estado { get; set; }
        public string estado { get; set; }
        public Nullable<double> tiempo_ans { get; set; }
    }
}
