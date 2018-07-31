using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    [BsonIgnoreExtraElements]
    public class PS_WS_ALTAS_SAP : ParentDto_ID_Auditoria
    {
        //Número de documento material
        public string Mblnr { get; set; }

        //Ejercicio del documento de material
        public int Mjahr { get; set; }

        //Posición en documento de material
        public int Zeile { get; set; }

        //Posición creada automáticamente
        public string Xauto { get; set; }

        //Número de material
        public string Matnr { get; set; }

        //Centro
        public string Werks { get; set; }

        //Almacén Origen
        public string Lgort { get; set; }

        //Almacen Receptor
        public string Umlgo { get; set; }

        //Número de lote
        public string Charg { get; set; }

        //Tipo stock
        public string Zustd { get; set; }

        //Indicador de stock especial
        public string Sobkz { get; set; }

        //Cantidad
        public int Menge { get; set; }

        //Unidad de medida base
        public string Meins { get; set; }

        //Destinatario de mercancías
        public string Wempf { get; set; }

        //Stock total valorado antes de la contabilización
        public string Lbkum { get; set; }

        //Valor del stock total valorado antes de la contabilización
        public decimal Salk3 { get; set; }

        //Indicador de control de precios
        public string Vprsv { get; set; }

        //Usuario de movimiento
        public string Usnam { get; set; }

        //Número de serie según el fabricante
        public string Serge { get; set; }

        //Número de serie
        public string Sernr { get; set; }

        public string correlationId { get; set; }

        public string estado { get; set; }

        public string Observaciones { get; set; }

        public int numero_intentos { get; set; }
    }
}
