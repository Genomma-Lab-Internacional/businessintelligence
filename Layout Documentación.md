Este layout pretende facilitar la creación de la documentación de código que se genere en el equipo de *Business Intelligence Internacional*. <br>

El objetivo es crear lineamientos de cómo presentar las herramientas programadas de forma fácil, entendible y sin la necesidad de detallar en el código para su uso.
 
# Datos Personales 

## Responsables del código

Esta información es relevante ya que si hay dudas o sugerencias respecto a la documentación o el código, se pueda establecer una comunicación directa con el (los) responsable (responsables).

- Nombre(s) de la(s) persona(s) que creó (crearon) el código y correo(s) del trabajo:

- Fecha de creación:

- Lenguaje de programación y versión:

Por ejemplo:

> | Nombres | Correos |
> |--------|--------|
> | Jesus Sabdiel Hernandez Morales | jesus.hernandez@genommalab.com |
> | Julio Ricardo Velazquez Hernandez | julio.velazquez@genommalab.com |

> Fecha de creación: 04-08-2021 <br>
> Desarrollado en Python 3.8.10


## Solicitantes

En este apartado se agrega la información del área y/o personas que solicitaron el desarrollo de la herramienta.

- Área que solicita:

- Persona(s) que solicita(n) y correo(s) del (de los) solicitante(s):

- Fecha de solicitud:


# Resumen

De 4 a 8 oraciones resumir la necesidad del negocio que hizo crear el código y cómo satisface esta necesidad. Por ejemplo:

> Los datos de SO y stock de los clientes B2B se almacenan en la plataforma del proveedor ISV.
> - Se necesita descargar los datos de SO y stock vía API.
> - Estos datos deben limpiarse y transformarse a un formato específico para cargarlos en un RDS.
> - Este script descarga los datos, los transformar y los carga en el RDS en un solo paso. Aunque se debe ejecutar el código manualmente.

# Código

## Nombre del script

Agregar la carpeta o en dónde podemos encontrar el código así como el nombre del archivo. Por ejemplo:

> Carpeta: [*ISV_Weekly*](https://github.com/Genomma-Lab-Internacional/businessintelligence/blob/master/ISV_Weekly) <br>
> Archivo: [*reproceso.py*](https://github.com/Genomma-Lab-Internacional/businessintelligence/blob/master/ISV_Weekly/reproceso.py)

## Parámetros Principales
En esta parte anotaremos los principales parámetros a modificar para que el código se pueda ejecutar y a qué se refiere cada uno. Por ejemplo:
> - *path*: Es la ubicación en dónde se exportará el backup de los datos a descargar.
> - *filename*: Es el nombre del archivo a exportar.
> - *week*: Es la semana objetivo del que se pretende descargar los datos.

## Conexiones e Importaciones

Se enlista qué conexiones necesita el código o si necesita importar datos de alguna otra fuente (*.xlsx, .csv, .json, .zip*, etc).

- Servidor:
- Base de datos:
- Tabla:
- Usuario (si es necesario):
- Contraseña (si es necesario):

Por ejemplo:

> Se ocupa la conexión al RDS con los siguientes parámetros:
> - Instancia: genommalab-businessintelligence
> - Host: genommalab-businessintelligence.cgkb305m9bb6.us-east-1.rds.amazonaws.com
> - Port: 3306
> - User: admin
> - Password: g3N0mmaLABi$ntelligence
> - DatabaseName: businessintelligence
> - Table: Fact_Desplazamiento_SemanalCol

## Explicación Detallada (Opcional)

Aquí se puede explicar a detalle el por qué del código o cómo funciona. Si se requiere, se puede agregar imagenes o multimedia.