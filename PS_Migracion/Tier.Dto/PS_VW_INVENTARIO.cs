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
    public class PS_VW_INVENTARIO : PS_PRODUCTO
    {
        [BsonElement("inventario")]
        public List<PS_INVENTARIO> inventario { get; set; }

        public Dto.PS_PRODUCTO getProducto()
        {
            return new PS_PRODUCTO
            {
                Id = this.Id,
                agrupador = this.agrupador,
                atributos_producto = this.atributos_producto,
                codigo = this.codigo,
                comentarios = this.comentarios,
                descripcion = this.descripcion,
                es_activo = this.es_activo,
                es_fisico = this.es_fisico,
                es_serializable = this.es_serializable,
                existencia_maxima = this.existencia_maxima,
                existencia_minima = this.existencia_minima,
                fabricante = this.fabricante,
                FechaActualizacion = this.FechaActualizacion,
                FechaCreacion = this.FechaCreacion,
                id_agrupador = this.id_agrupador,
                id_ramo = this.id_ramo,
                id_tipo_material = this.id_tipo_material,
                id_unidad_medida = this.id_unidad_medida,
                modelo = this.modelo,
                ramo = this.ramo,
                tipo_material = this.tipo_material,
                unidad_medida = this.unidad_medida,
                UsuarioCreacion = this.UsuarioCreacion,
                UsuarioModificacion = this.UsuarioModificacion
            };
        }

        public Dto.PS_INVENTARIO getInventario(string idElemento)
        {
            return inventario.Where(i => i.Id == idElemento).FirstOrDefault();
        }
    }
}
