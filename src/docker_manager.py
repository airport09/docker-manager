import os
import time
import docker
import base64
import boto3 as boto
from pathlib import Path
from ast import literal_eval
from typing import Dict, Union, List

from src.utils.logging_ import get_logger
from src.utils.filing import (
    set_json,
    get_json,
)

from src.constants import (ALL,
                           IMAGES,
                           CONTAINERS,
                           DEFAULT_PORT)

from src.utils.exceptions_handling import credentials_not_found


class DockerManager:
    def __init__(self,
                 verbose: bool = False,
                 debug: bool = False):

        self.client = self.instantiate_client()

        self.logger = get_logger(debug=debug,
                                 silent=not verbose)

        self.tag = None

    @staticmethod
    def instantiate_client() -> docker.client.DockerClient:
        return docker.from_env()

    @staticmethod
    def get_aws_userid(region: str) -> str:
        return boto.client(
            "sts",
            region_name=region,
        ).get_caller_identity()["Account"]

    @staticmethod
    def create_ecr_client(aws_region: str):
        return boto.client(
            'ecr',
            region_name=aws_region,
        )

    @staticmethod
    def get_ecr_credentials(
            aws_region,
            credentials={},
    ) -> dict:
        if not credentials:
            ecr_client = DockerManager.create_ecr_client(
                aws_region,
            )
            ecr_credentials = (
                ecr_client
                .get_authorization_token()
                ['authorizationData'][0]
            )

            credentials['username'] = 'AWS'

            credentials['password'] = (
                base64.b64decode(ecr_credentials['authorizationToken'])
                .replace(b'AWS:', b'')
                .decode('utf-8')
            )

            credentials['url'] = ecr_credentials['proxyEndpoint']

            return credentials

    @staticmethod
    def reset_credentials(file_path: Union[Path, str] = '.docker/config.json') -> None:

        folder_path = Path.home() / Path(file_path).parent
        full_path = Path.home() / Path(file_path)

        folder_path.mkdir(exist_ok=True)

        set_json(
            path=full_path,
            dictionary={},
        )

        assert get_json(full_path) == {}

    @property
    def is_sagemaker(self) -> bool:

        home_dir = [
            instance.name.lower()
            for instance in Path.home().iterdir()
        ]

        return 'sagemaker' in home_dir

    @property
    def running_containers(self) -> list:
        """ Equivalent to command docker ps """
        return self.client.containers.list()

    @property
    def all_containers(self) -> list:
        """ Equivalent to command docker ps --all """
        return self.client.containers.list(all=True)

    @staticmethod
    def ask_to_stop_container(
            reason: str,
            name: str) -> bool:
        message = f"Container With {reason} -> {name} <- Is Running.\n" \
                  f"Stop the Container? [y/n] "
        response = input(message)

        if response.lower() in ['y', 'yes']:
            return True

    @staticmethod
    def ask_to_clear_containers() -> bool:
        message = "For ALL the IMAGES to be removed need to stop " \
                  "and remove all the containers! Proceed? [y/n] "
        response = input(message)

        if response.lower() in ['y', 'yes']:
            return True

    @staticmethod
    def ask_to_build_image(name: str) -> bool:
        message = f"There is no Image -> {name} <-.\nBuild one? [y/n] "
        response = input(message)

        if response.lower() in ['y', 'yes']:
            return True

    @staticmethod
    def chmod_x(file) -> None:
        mode = os.stat(file).st_mode
        mode |= (mode & 0o444) >> 2
        os.chmod(file, mode)

    @staticmethod
    def make_executable(files) -> None:
        if not files:
            return

        if isinstance(files, str):
            files = [files]

        for file in files:
            DockerManager.chmod_x(file)

    @staticmethod
    def prettify_push_log(
            push_log: str,
    ) -> str:
        events = push_log.split("\n")
        needed_events = [
            event for event in events
            if event and literal_eval(event).get("status") in [
                "Pushed",
                "Layer already exists",
            ]
        ]

        return "\n".join(needed_events) + events[-3]

    def remove_image(self,
                     image_name: str) -> None:
        if self.list_images(name=image_name):
            self.client.images.remove(image_name)

    def remove_all_images(self) -> None:
        for image in self.list_images():
            try:
                self.client.images.remove(
                    image.id,
                    force=True,
                )
            except Exception as e:
                self.logger.error(f'Can not remove {image.id}')
                self.logger.error(e)
        self.logger.info('ALL IMAGES ARE REMOVED')

    def stop_all_containers(self) -> None:
        for container in self.running_containers:
            container.stop()

        self.logger.info('ALL CONTAINERS ARE STOPPED')

    def remove_all_containers(self) -> None:
        self.client.containers.prune()

        self.logger.info('ALL CONTAINERS ARE REMOVED')

    def list_images(
            self,
            name=None,
    ) -> list:
        return self.client.images.list(name=name)

    def list_containers(self,
                        image_name=None) -> list:
        return self.client.containers.list(
            filters={"ancestor": image_name},
        )

    def check_image(self,
                    image: str) -> None:
        if not self.list_images(name=image):
            self.handle_non_existing_image(image=image)

    def check_image_containers(self,
                               image: str) -> None:
        if self.list_containers(image):
            self.handle_running_container(
                reason='image',
                name=image,
                for_image=True,
            )

    def handle_non_existing_image(self,
                                  image: str) -> None:
        confirm = self.ask_to_build_image(name=image)
        if confirm:
            self.build_image(image)
        else:
            self.logger.warning('CAN NOT PUSH NON-EXISTING IMAGE')
            exit()

    def build_image(
            self,
            tag,
            executable_files: Union[str, list, Path] = None,
            docker_file_loc: Union[str, Path] = '.',
    ) -> None:

        self.check_image_containers(tag)
        self.tag = tag

        self.logger.info('BUILDING...')
        self.make_executable(executable_files)
        self.client.images.build(
            path=docker_file_loc,
            tag=tag,
        )

        self.logger.info('BUILT')
        assert len(self.list_images(name=tag)) == 1

    def construct_push_name(
            self,
            user_id: str,
            region: str,
            image: str,
            tag: str,
    ) -> str:
        if not user_id:
            user_id = self.get_aws_userid(region=region)

        return f"{user_id}.dkr.ecr.{region}.amazonaws.com/{image}:{tag}"

    def tag_image(
            self,
            user_id: str,
            region: str,
            image: str,
            tag: str,
    ) -> str:
        """Example docker tag 0e5574283393 fedora/httpd: version1.0 """

        image_object = self.client.images.get(f"{image}:{tag}")
        push_name = self.construct_push_name(
            user_id, region,
            image, tag,
        )

        image_object.tag(push_name)
        self.logger.info(f'Image Tagged -> {push_name}')
        return push_name

    def authenticate_docker(self,
                            aws_region: str) -> None:
        credentials = self.get_ecr_credentials(aws_region)
        if self.is_sagemaker:
            self.reset_credentials()

        self.client.login(
            username=credentials['username'],
            password=credentials['password'],
            registry=credentials['url'],
        )

        self.logger.info('Authenticated!')

    @credentials_not_found
    def push_image_to_ecr(
            self,
            aws_region: str,
            image: str = None,
            tag: str = 'latest',
            user_id: str = None,
    ) -> str:

        if not image:
            image = self.tag

        self.check_image(image)

        self.logger.info('PUSHING TO ECR...')
        uri = self.tag_image(
            user_id,
            aws_region,
            image,
            tag,
        )

        self.authenticate_docker(aws_region)
        push_log = self.client.images.push(uri)

        self.logger.info('PUSHED')
        self.logger.debug(
            '\n' + self.prettify_push_log(push_log),
        )
        return uri

    def stop_container(
            self,
            id_: str = None,
            image: str = None,
    ) -> None:
        if id_:
            self.client.containers.get(id_).stop()
            self.logger.info('Running Container Is Stopped')
        elif image and self.list_containers(image):
            self.list_containers(image)[0].stop()
            self.logger.info('Running Container Is Stopped')
        else:
            self.logger.info('No Such Running Container')

    def handle_running_container(
            self,
            reason: str,
            name: str,
            id_: str = None,
            for_image: bool = False,
    ) -> None:

        confirmed = self.ask_to_stop_container(reason, name)

        if confirmed and id_:
            self.stop_container(id_=id_)
        elif confirmed and not id_:
            self.stop_container(image=name)
        elif not confirmed and for_image:
            self.logger.debug(
                'CONTAINER IS NOT STOPPED\n'
                'It will be deleted before creating container.',
            )
        else:
            self.logger.critical(
                'YOU REFUSED TO STOP CONTAINER.\n'
                'CONTAINER IS STILL RUNNING',
            )
            exit()

    def run_container_and_serve(
            self,
            image: str,
            ports: Dict,
            detach: bool,
            command: str,
            auto_remove: bool,
            env_variables: Union[List, Dict],
    ) -> None:
        self.client.containers.run(
            image=image,
            ports=ports,
            detach=detach,
            command=command,
            auto_remove=auto_remove,
            environment=env_variables,
        )

    def id_from_port(self, port: dict) -> str:
        return [
            cont.id
            for cont in self.running_containers
            if port.keys() == cont.ports.keys()
        ][0]

    def free_ports(self, port_to_check: dict) -> bool:
        used_ports = [
            cont.ports.keys()
            for cont in self.running_containers
        ]
        if port_to_check.keys() not in used_ports:
            return True

    def check_container(
            self,
            image,
            ports,
    ) -> None:
        if len(self.list_containers(image)) == 1:
            self.handle_running_container(
                reason='image',
                name=image,
            )

        elif not self.free_ports(ports):
            self.handle_running_container(
                reason='port',
                name=ports,
                id_=self.id_from_port(ports),
            )

    def create_local_container(
            self,
            image: str = None,
            command: str = None,

            detach: bool = True,
            auto_remove: bool = True,
            ports: Dict = DEFAULT_PORT,

            env_variables: Union[List, Dict] = None,
    ) -> None:
        if not image:
            image = self.tag

        self.check_container(image, ports)

        self.logger.info('CREATING LOCAL CONTAINER...')
        self.run_container_and_serve(
            image=image,
            ports=ports,
            detach=detach,
            command=command,
            auto_remove=auto_remove,
            env_variables=env_variables,
        )

        time.sleep(1)
        self.logger.info('LOCAL CONTAINER IS CREATED')

    def clean_up(self, obj: str) -> None:
        if obj == IMAGES:
            if self.ask_to_clear_containers():
                self.clean_up(obj=ALL)
            else:
                self.remove_all_images()

        elif obj == CONTAINERS:
            self.stop_all_containers()
            self.remove_all_containers()

        elif obj == ALL:
            self.stop_all_containers()
            self.remove_all_containers()
            self.remove_all_images()
