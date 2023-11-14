import os

import virtualenv


SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


def setup_env(venv_name, req_file):
    virtualenvs_folder = os.path.expanduser("~/.virtualenvs")
    venv_dir = os.path.join(virtualenvs_folder, venv_name)
    virtualenv.cli_run([venv_dir])
    cmd = f". {virtualenvs_folder}/{venv_name}/bin/activate && pip3 install -r {req_file} && pip3 install {SCRIPT_PATH}"
    os.system(cmd)


def prepare_hadoop():
    os.system(f"sudo -u hdfs hadoop fs -mkdir /user/{os.environ['USER']}")
    os.system(f"sudo -u hdfs hadoop fs -chown {os.environ['USER']} /user/{os.environ['USER']}")


if __name__ == '__main__':
    setup_env(
        venv_name='address_list_from_osm',
        req_file=f'{SCRIPT_PATH}/requirements.txt'
    )

    prepare_hadoop()
