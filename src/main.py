import requests
import time
import json
import boto3
import os
import sys
from pydub import AudioSegment
from dotenv import load_dotenv
import zipfile
import logging
load_dotenv()

link = os.getenv('storage_link') + os.getenv('bucket_name')

header = {'Authorization': 'Api-Key {}'.format(os.getenv('api_key'))}

FORMAT = '%(asctime)s: %(levelname)s: %(name)s %(module)s: %(message)s'
filename_log = str(os.getenv('LOGS_PATH') + os.getenv('FILENAME_LOG'))


def get_logger(name, level=logging.INFO) -> logging.Logger:
    logging.basicConfig(format=FORMAT,
                        level=level,
                        filename=filename_log,
                        encoding='utf-8')
    logger = logging.getLogger(name)
    return logger


# in any file that import fn get_logger, you can set up local logger like:
local_logger = get_logger(__name__)


def convert_wav_to_ogg(file_path, file_ogg_path):
    audio = AudioSegment.from_wav(file_path)
    audio.export(file_ogg_path, format="ogg")
    local_logger.info(f"convert to ogg {file_ogg_path}")
    return file_ogg_path


def parse_wav_to_ogg():
    with os.scandir(os.getenv('FILES_PATH') + '/') as it:
        for entry in it:
            if not os.path.splitext(entry.name)[-1]:
                for file in os.scandir(entry):
                    if os.path.splitext(file)[-1] != '.json':
                        file_ogg_path = os.path.splitext(file)[0] + '.ogg'
                        if not os.path.exists(file_ogg_path):
                            convert_wav_to_ogg(file, file_ogg_path)


def connect_session():
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=os.getenv('endpoint'),
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        region_name='ru-central1'
    )
    local_logger.info(f"Connect to session: {s3}")
    return s3


def unzip_files(archive):
    with zipfile.ZipFile(archive, 'r') as zip_file:
        zip_file.extractall(os.path.splitext(archive)[0])
        local_logger.info(f"Unzip archive: {archive}")


def unzip():
    with os.scandir(os.getenv('FILES_PATH') + '/') as it:
        for entry in it:
            if not os.path.exists(os.path.splitext(entry)[0]):
                os.mkdir(os.path.splitext(entry)[0])
            if os.path.splitext(entry.name)[-1] == '.zip':
                unzip_files(os.getenv('FILES_PATH') + '/' + entry.name)


def upload_to_bucket(s3, file_ogg_path):
    s3.upload_file(file_ogg_path, os.getenv('name_bucket'), file_ogg_path)


def save_body_list():
    body_list = list()
    with os.scandir(os.getenv('FILES_PATH') + '/') as it:
        for entry in it:
            if not os.path.splitext(entry.name)[-1]:
                for file in os.scandir(entry):
                    if os.path.splitext(file)[-1] == '.ogg':
                        file_ogg = file.name
                        body_list.append({
                            "config": {
                                "specification": {
                                    "languageCode": "ru-RU",
                                    "model": "deferred-general"
                                }
                            },
                            "audio": {
                                "uri": link + file_ogg
                            }
                        })
    return body_list


def save_json_recognition(req, filename):
    with open(f'{filename}.json', 'w', encoding='utf-8') as file:
        json.dump(req, file, ensure_ascii=False, indent=4)
    local_logger.info(f"Save json recognition")
    return f'{filename}.json'


def save_text_recognition(req, filename):
    with open(f'{filename}.txt', 'w', encoding='utf-8') as file:
        for chunk in req['response']['chunks']:
            file.write(chunk['alternatives'][0]['text'])


def post_request(body):
    req = requests.post(os.getenv('POST'), headers=header, json=body)
    data = req.json()
    local_logger.info(f"data: {data}")
    print(data)
    id = data['id']
    step = 30
    tt = 0
    # Запрашивать на сервере статус операции, пока распознавание не будет завершено.
    while True:

        time.sleep(step)
        tt = tt + step

        GET = "https://operation.api.cloud.yandex.net/operations/{id}"
        req = requests.get(GET.format(id=id), headers=header)
        req = req.json()

        if req['done']:
            break
        local_logger.info(f"Not ready {str(tt)}")

    # Показать только текст из результатов распознавания.
    local_logger.info("Text chunks:")

    for chunk in req['response']['chunks']:
        print(chunk['alternatives'][0]['text'])

from enum import Enum
class Demos(Enum):
    VARIABLE1 = "Car"
    VARIABLE2 = "Bus"
    VARIABLE3 = "Example"
    VARIABLE4 = "Example2"


class AbonentType(Enum):
    Operator = 1
    Abonent = 2
    Specialist = 999

if __name__ == '__main__':
    unzip()
    parse_wav_to_ogg()
    body_list = save_body_list()
    print(body_list)
    print(body_list[0])
    """    con = connect_session()
        post_request(body_list[0])"""
    for variable in Demos:
        print(variable.value)
    mas = [1, 2, 1, 2, 999, 3, 4, 6, 999, 2, 1]
    for i in mas:
        for abonent in AbonentType:
            if i == abonent.value:
                print(abonent.value)


