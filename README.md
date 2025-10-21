
# HistoryExportCmd

`HistoryExportCmd` es una aplicación de consola .NET 4.0 diseñada para exportar datos históricos de un sistema EBI a una base de datos SQL Server dedicada (`PointsHistory`). Incluye lógica para operar en un entorno de servidores redundantes.

**Estado del Proyecto:** Esta es la recuperación de un proyecto a partir de un ejecutable .NET decompilado. El código fuente original se perdió. El código ha sido estabilizado, pero no fue escrito originalmente para este repositorio.

## Arquitectura y Funcionalidad Principal

La aplicación opera en uno de dos modos, determinados en el arranque:

1.  **Modo Primario:** Si la aplicación detecta que se está ejecutando en el servidor EBI primario (llamando con éxito a `hwsystem.dbo.hsc_sp_IsPrimary` o `hsc_mfn_IsPrimary`), procederá a:
    * Leer las configuraciones de puntos desde la base de datos local `PointsHistory` (tabla `Point`).
    * Conectarse a la base de datos de históricos de EBI vía **ODBC** (`EBI_ODBC`).
    * Obtener datos de las tablas `History5SecondSnapshot`, `History1MinSnapshot` y `History1HourSnapshot` basándose en la última marca de tiempo registrada.
    * Insertar estos datos en las tablas correspondientes `History_5sec`, `History_1min` y `History_1hour` en la base de datos SQL `PointsHistory`.
    * Ejecutar un paso de agregación para poblar `History_15min` a partir de `History_1min`.

2.  **Modo Secundario (Sincronización):** Si el servidor EBI no es primario y `RedundantPointHistory` está habilitado en `app.config`, procederá a:
    * Asumir un servidor pareado (ej. "SERVERA" y "SERVERB").
    * Conectarse a la base de datos `PointsHistory` del servidor *primario* (probablemente a través de un servidor vinculado).
    * Sincronizar la tabla `Point` y todas las tablas `History_*` trayendo los registros faltantes desde el primario a la base de datos secundaria local.

## Configuración (app.config)

Toda la configuración se gestiona en `app.config`.

### Cadenas de Conexión
* `PointsHistory`: Cadena de conexión de SQL Server para la base de datos local donde se almacenan los históricos.
* `EBI_ODBC`: El Nombre de Origen de Datos (DSN) de ODBC para conectarse a las tablas de snapshots de históricos de EBI.
* `EBI_SQL`: Cadena de conexión de SQL Server a la base de datos `master` local de EBI, usada *solo* para comprobar el estado del servidor primario.

### App Settings
* `OldestDayFromToday`: El número máximo de días en el pasado para consultar históricos si la base de datos local está vacía (ej. `1295` días).
* `RedundantPointHistory`: `true`/`false`. Habilita o deshabilita el Modo Secundario (Sincronización).
* `LogPath`: Directorio para almacenar los archivos de log (ej. `Logs`).

## Clases Clave
* `Program.cs`: Punto de entrada principal. Contiene la lógica de alto nivel para el modo Primario/Secundario.
* `DBAccess.cs`: Gestiona todas las operaciones de base de datos para el **Modo Primario** (leer de EBI ODBC, escribir en `PointsHistory` SQL).
* `DBSync.cs`: Gestiona todas las operaciones de base de datos para el **Modo Secundario** (sincronizar `PointsHistory` desde el servidor primario).
* `LogFile.cs`: Un registrador (logger) de archivos personalizado y simple.
* `Point.cs` / `History.cs`: Modelos de datos para la configuración de puntos y los registros de históricos.