﻿using System;
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
            //Classes.CorreccionUsuarios.CorreccionGrupoUsuarios();
            //Classes.Extractores.Extractor_PS_APROBACION_Usuario();
            Classes.Extractores.Extractor_PS_SERVICIO_CLIENTE();
            //Classes.Extractores.Extractor_PS_APROVISIONAMIENTO();
            //Classes.Extractores.Extractor_PS_VIABILIDAD();
            //Classes.Extractores.Extractor_PS_TAREA_SOLICITUD();
            //Classes.Extractores.Extractor_PS_SERVICIO_CLIENTE_valores_elementos_configuracion("5b61d9f8bf886f0bc0b7a641");
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

            if (sOpcion == "Extraccion_Colecciones_PS")
            {
                //Classes.Extractores.Extractor_PS_ALERTA(sOpcion2); 
                
                Classes.Extractores.Extractor_PS_APROVISIONAMIENTO();               
                Classes.Extractores.Extractor_PS_SERVICIO_CLIENTE();
                Classes.Extractores.Extractor_PS_TAREA_SOLICITUD();
                Classes.Extractores.Extractor_PS_VIABILIDAD();
                

            }
        }
    }
}
