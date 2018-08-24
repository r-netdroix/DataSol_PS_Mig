using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tier.Dto;

namespace Tier.Cmd
{
    class Program
    {
        static void Main(string[] args)
        {
            string sOpcion = "";
            //Classes.PS_CONFIG_CAMPOS_RESPUESTA_PRODUCTO.RecolectarDatos();
            //Classes.PS_USUARIO.RecolectarDatos();
            Classes.CorreccionUsuarios.CorreccionGrupoUsuarios();

            try
            {
                sOpcion = args[0];
            }
            catch { }

            string sOpcion2 = "";
            try
            {
                sOpcion2 = args[1];
            }
            catch { }

            if (sOpcion == "Extraccion_Colecciones_PROM")
            {
                Classes.Extractores.Extractor_PS_ALERTA(sOpcion2);                
            }
        }
    }
}
