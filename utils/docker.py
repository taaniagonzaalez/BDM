import utils.docker

def build_docker_image(image_tag, dockerfile_path="."):
    client = utils.docker.from_env()
    print(f"Construyendo la imagen {image_tag}...")
    image, logs = client.images.build(path=dockerfile_path, tag=image_tag)
    for log in logs:
        if 'stream' in log:
            print(log['stream'].strip())
    print(f"Imagen {image_tag} creada exitosamente.")

def run_docker_container(image_tag, container_name):
    client = docker.from_env()
    print(f"Iniciando contenedor {container_name} desde la imagen {image_tag}...")
    container = client.containers.run(image_tag, name=container_name, detach=True)
    print(f"Contenedor {container_name} en ejecución con ID {container.short_id}.")
    return container

if __name__ == "__main__":
    IMAGE_TAG = "mi_imagen_docker"
    CONTAINER_NAME = "mi_contenedor"
    
    build_docker_image(IMAGE_TAG)
    run_docker_container(IMAGE_TAG, CONTAINER_NAME)