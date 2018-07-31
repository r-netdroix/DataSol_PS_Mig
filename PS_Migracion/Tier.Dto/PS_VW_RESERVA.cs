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
    public class PS_VW_RESERVA
    {
        private ObjectId _id_inventario;
        private ObjectId _id_producto;
        private ObjectId _id_estado;
        private ObjectId _id_bodega;
        private ObjectId _id_aprovisionamiento;
        private ObjectId _id_ramo;
        private ObjectId _id_tipo_material;
        private ObjectId _id_unidad_medida;
        private ObjectId _id_agrupador;
        private ObjectId _id_accion;


        #region Inventario

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string _id
        {
            get { return Convert.ToString(_id_inventario); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_inventario); }
        }

        public string serial { get; set; }

        public double cantidad { get; set; }

        public double cantidad_reserva { get; set; }

        public string lote { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_estado
        {
            get { return Convert.ToString(_id_estado); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_estado); }
        }

        public string estado { get; set; }

        public string centro_costo { get; set; }

        public double valor_unitario { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_bodega
        {
            get { return Convert.ToString(_id_bodega); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_bodega); }
        }

        public string bodega { get; set; }

        public string ubicacion { get; set; }

        public Nullable<bool> es_activo { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public DateTime fecha_creacion { get; set; }

        public string usuario_creacion { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
        public DateTime fecha_actualizacion { get; set; }

        public string usuario_modificacion { get; set; }

        #endregion


        #region Producto

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_producto
        {
            get { return Convert.ToString(_id_producto); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_producto); }
        }

        public string codigo { get; set; }

        public string descripcion { get; set; }

        public string fabricante { get; set; }

        public string modelo { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_ramo
        {
            get { return Convert.ToString(_id_ramo); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_ramo); }
        }

        public string ramo { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_tipo_material
        {
            get { return Convert.ToString(_id_tipo_material); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_tipo_material); }
        }

        public string tipo_material { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_unidad_medida
        {
            get { return Convert.ToString(_id_unidad_medida); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_unidad_medida); }
        }

        public string unidad_medida { get; set; }

        public string comentarios { get; set; }

        public decimal existencia_minima { get; set; }

        public decimal existencia_maxima { get; set; }

        public Nullable<bool> prod_es_activo { get; set; }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_agrupador
        {
            get { return Convert.ToString(_id_agrupador); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_agrupador); }
        }

        public string agrupador { get; set; }

        public Nullable<bool> es_fisico { get; set; }

        public Nullable<bool> es_serializable { get; set; }

        #endregion

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_aprovisionamiento
        {
            get { return Convert.ToString(_id_aprovisionamiento); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_aprovisionamiento); }
        }

        [BsonIgnoreIfDefault]
        [BsonRepresentation(BsonType.ObjectId)]
        public string id_accion
        {
            get { return Convert.ToString(_id_accion); }
            set { MongoDB.Bson.ObjectId.TryParse(value, out _id_accion); }
        }

        public double cantidad_salida { get; set; }

        public string documento { get; set; }

        public string posicion { get; set; }

        public Dto.PS_PRODUCTO getProducto()
        {
            return new PS_PRODUCTO
            {
                Id = this.id_producto,
                agrupador = this.agrupador,
                atributos_producto = new List<PS_ATRIBUTO_PRODUCTO>(),
                codigo = this.codigo,
                comentarios = this.comentarios,
                descripcion = this.descripcion,
                es_activo = this.prod_es_activo,
                es_fisico = this.es_fisico,
                es_serializable = this.es_serializable,
                existencia_maxima = this.existencia_maxima,
                existencia_minima = this.existencia_minima,
                fabricante = this.fabricante,
                FechaActualizacion = this.fecha_actualizacion,
                FechaCreacion = this.fecha_creacion,
                id_agrupador = this.id_agrupador,
                id_ramo = this.id_ramo,
                id_tipo_material = this.id_tipo_material,
                id_unidad_medida = this.id_unidad_medida,
                modelo = this.modelo,
                ramo = this.ramo,
                tipo_material = this.tipo_material,
                unidad_medida = this.unidad_medida,
                UsuarioCreacion = this.usuario_creacion,
                UsuarioModificacion = this.usuario_modificacion
            };
        }

        public Dto.PS_INVENTARIO getElemento()
        {
            return new PS_INVENTARIO
            {
                Id = this._id,
                accion_inventario = new List<PS_ACCION_INVENTARIO>(),
                bodega = this.bodega,
                cantidad = this.cantidad,
                cantidad_reserva = this.cantidad_reserva,
                centro_costo = this.centro_costo,
                estado = this.estado,
                id_bodega = this.id_bodega,
                id_estado = this.id_estado,
                id_producto = this.id_producto,
                lote = this.lote,
                producto = this.descripcion,
                serial = this.serial,
                ubicacion = this.ubicacion,
                valor_unitario = this.valor_unitario,
                es_activo = this.es_activo,
                FechaActualizacion = this.fecha_actualizacion,
                FechaCreacion = this.fecha_creacion,
                UsuarioCreacion = this.usuario_creacion,
                UsuarioModificacion = this.usuario_modificacion,
                posicion = this.posicion,
                documento = this.documento
            };
        }

    }
}

