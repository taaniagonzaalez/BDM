import os
import subprocess

# Configurar la variable de entorno para la ubicación de Airflow
AIRFLOW_HOME = os.path.expanduser("~/airflow")
os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME

def install_airflow():
    """Instala Apache Airflow y sus dependencias."""
    print("📦 Instalando Apache Airflow...")
    subprocess.run(["pip", "install", "apache-airflow"], check=True)
    print("✅ Airflow instalado.")

def initialize_airflow():
    """Inicializa Airflow y ejecuta sus servicios."""
    print("🔄 Inicializando la base de datos de Airflow...")
    subprocess.run(["airflow", "db", "init"], check=True)
    
    print("🚀 Creando usuario administrador...")
    subprocess.run([
        "airflow", "users", "create",
        "--username", "admin",
        "--password", "admin",
        "--firstname", "Admin",
        "--lastname", "User",
        "--role", "Admin",
        "--email", "admin@example.com"
    ], check=True)

    print("🖥️ Iniciando Airflow Webserver en el puerto 8080...")
    subprocess.Popen(["airflow", "webserver", "--port", "8080"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print("📅 Iniciando Airflow Scheduler...")
    subprocess.Popen(["airflow", "scheduler"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print("🎉 Airflow está corriendo en http://localhost:8080 (Usuario: admin, Contraseña: admin)")

if __name__ == "__main__":
    install_airflow()
    initialize_airflow()
