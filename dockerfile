
FROM apache/airflow:2.10.2-python3.8

WORKDIR /opt/airflow

# Copiar el archivo de requerimientos
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de los archivos del proyecto
COPY . .

# Cambiar de nuevo a usuario airflow
USER airflow

# Inicializar la base de datos de Airflow
RUN airflow db init

# Comando para iniciar el servidor web de Airflow
CMD ["airflow", "webserver"]
