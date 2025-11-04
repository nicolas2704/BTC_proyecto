# BTC_proyecto
Proyecto de Ingenieria de Datos relacionado a Bitcoin
El proyecto, esta diseñado para ser ejecutado en el orquestador de Airflow y el archivo .py principal, llamado: btc_github_proy.py, este necesita que las variables de entorno sean cargadas. 
Por tema de seguridad no subo dichos datos ya que son privados e incluyen contraseñas.
Al ejecutar el DAG en Airflow, lo que realiza es una extraccion de precios en dolares del activo Bitcoin, de los ultimos 30 dias y extrae datos en bruto, los cuales necesitan ser transformado. Estos son exportados en una hoja de calculo excel llamada: btc_datos_extraidos.xlsx donde se pueden ver.
La etapa de transformacion permite cambiar formato de fecha y el tipo de datos de los precios, asi tambien como filtrar: apertura, cierre, maximos y minimos.
Se crea una funcion llamada enviar_email donde se desarrolla la alerta en caso de que BTC rompa su maximo precio anterior. Esta esta asociada a otra funcion la cual evalua las condiciones planteadas
El flujo de carga permite que el Dataframe generado sea cargado en la base de datos proyecto que pertenece a un sistema de gestion PostgreSQL.
Los datos cargados pueden ser consultados con SQL cuando sean necesarios
Cada tarea y en el caso de haber algun error todo queda registrado en un archivo llamado: btc_registro.txt
Para finalizar se encunetra un archivo llamda grafico_BTC el cual al ser ejecutado hace una consulta a la base de datos, trayendo los datos disponibles de BTC y construyendo una app web con DASH para poder visualizar los precios en un grafico interactivo de velas japonesas
TODO ESTO SE PUEDE OBSERVAR DE UNA FORMA VISUAL EN EL MI PERFIL DE LINKEDIN, EN LA SECCION DE PROYECTOS.
