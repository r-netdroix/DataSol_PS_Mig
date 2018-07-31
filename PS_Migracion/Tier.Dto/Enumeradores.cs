using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tier.Dto
{
    public class Enumeradores
    {
        public struct TipoContacto
        {
            public const string Administrativo = "ADMINISTRIVO";
            public const string Tecnico = "TECNICO";
        }

        public struct TipoAccion
        {
            public const string Crear = "Crear";
            public const string Reservar = "Reservar";
            public const string Liberar = "Liberar";
            public const string Ocupar = "Ocupar";
            public const string Reasignar = "Reasignar";
            public const string Reversar = "Reversar";
            public const string Trasladar = "Trasladar";
            public const string Causar = "Causar";
            public const string Baja = "Baja";
            public const string Devolucion = "Devolución";
        }

        public struct TipoMovimiento
        {
            public const string Entradas = "Entrada";
            public const string Salidas = "Salida";
            public const string Bajas = "Baja";
        }

        public struct TipoCampoValidacionParametros
        {
            public const string Instalacion = "TipoInstalacion";
            public const string TipoSolicitud = "TipoSolicitud";
            public const string TipoOperacion = "TipoOperacion";
            public const string TipoOportunidad = "TipoOportunidad";
            public const string Producto = "Producto";
            public const string Segmento = "Segmento";
        }

        public enum OpcionesSeleccionMultiple : byte
        {
            CambiarEstado,
            Eliminar,
            CambiarLider,
            CerrarTarea,
            AsignaPropietario
        }

        public enum TipoSolicitud : byte
        {
            VIABILIDAD,
            APROVISIONAMIENTO
        }

        public enum AccionesHistoricoRegitros : byte
        {
            Crear,
            Editar,
            Eliminar,
            CambiarEstado,
            AsignarResponsable,
            ReasignarResponsable,
            AgregarAdjunto,
            AsociarTareaSolicitud,
            FinalizarTarea,
            EnvioComunicacion,
            NuevoAdjunto,
            Reservar,
            Reasignar,
            Liberar,
            Guardar,
            SolicitarEntregaMateriales,
            ConfirmarInstalacion
        }

        public enum TipoElemento : byte
        {
            Fisico,
            Logico
        }

        public enum PanelGestion : byte
        {
            Administrador,
            Almacenista,
            Aprobador,
            Lider,
            Propietario
        }

        public enum EntidadesConsecutivos : byte
        {
            VIABILIDAD,
            APROVISIONAMIENTO,
            SERVICIO_CLIENTE
        }
    }
}
