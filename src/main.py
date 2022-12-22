import requests
import time
import json
import boto3
import os
from pydub import AudioSegment
from dotenv import load_dotenv
import zipfile
import logging
import difflib
load_dotenv()

link = os.getenv('storage_link') + os.getenv('bucket_name') + "/"
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


def upload_to_bucket(s3, file_wav_path, path):
    key = 'FILES' + '/' + path + '/'
    print(key+file_wav_path)
    print(key)
    s3.put_object(Bucket=os.getenv('bucket_name'), Key=key)
    s3.upload_file(key+file_wav_path, os.getenv('bucket_name'), key+file_wav_path)


def save_body_list():
    body_list = list()
    with os.scandir(os.getenv('FILES_PATH') + '/') as it:
        for entry in it:
            if not os.path.splitext(entry.name)[-1]:
                for file in os.scandir(entry):
                    if os.path.splitext(file)[-1] == '.wav':
                        file_wav = str(os.path.dirname(file) + "/" + os.path.basename(file))
                        body_list.append({
                            "config": {
                                "specification": {
                                    "languageCode": "ru-RU",
                                    "audioEncoding": "LINEAR16_PCM",
                                    "model": "deferred-general",
                                    "sampleRateHertz": 8000,
                                    "rawResults": 'true'
                                }
                            },
                            "audio": {
                                "uri": link + file_wav
                            }
                        })
    return body_list


def save_json_recognition(req, path, filename):
    if not os.path.exists(os.getenv('RECOGNITION_FILES_PATH') + str(path[0]) + '/'):
        os.mkdir(os.getenv('RECOGNITION_FILES_PATH') + str(path[0]) + '/')
    with open(os.getenv('RECOGNITION_FILES_PATH') + str(path[0]) + '/' + f'{filename}.json', 'w', encoding='utf-8') as file:
        json.dump(req, file, ensure_ascii=False, indent=4)
    local_logger.info(f"Save json recognition")
    return f'{filename}.json'


def save_text_recognition(req, path, filename):
    if not os.path.exists(os.getenv('RECOGNITION_FILES_PATH') + str(path[0]) + '/'):
        os.mkdir(os.getenv('RECOGNITION_FILES_PATH') + str(path[0]) + '/')
    try:
        with open(os.getenv('RECOGNITION_FILES_PATH') + str(path[0]) + '/' + f'{filename}.txt', 'w', encoding='utf-8') as file:
            for chunk in req['response']['chunks']:
                file.writelines(chunk['alternatives'][0]['text'])
    except Exception as e:
        local_logger.error(e, exc_info=True)


def file_to_json_data(file):
    with open(file, 'r', encoding='utf-8') as read_file:
        data = json.load(read_file)
    return data


def parse_json_yandex(rec_json, path, filename):
    try:
        with open(os.getenv('RECOGNITION_FILES_PATH') + str(path) + '/' + f'{filename}.txt', 'w',
                  encoding='utf-8') as file:
            for chunk in rec_json['response']['chunks']:
                for word in chunk['alternatives'][0]['words']:
                    file.writelines(word['word'] + '\n')
            return file.name
    except Exception as e:
        local_logger.error(e, exc_info=True)

def parse_json_vosk(rec_json, path):
    filename = os.path.splitext(rec_json['filename'])[0]
    try:
        with open(os.getenv('RECOGNITION_FILES_PATH') + str(path) + '/' + f'{filename}_vosk.txt', 'w',
                  encoding='utf-8') as file:
            for words in rec_json['result']:
                for word in words['words']:
                    file.writelines(word['word'] + '\n')
            return file.name
    except Exception as e:
        local_logger.error(e, exc_info=True)



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

        GET = f"https://operation.api.cloud.yandex.net/operations/{id}"
        req = requests.get(GET.format(id=id), headers=header)
        req = req.json()

        if req['done']:
            break
        print("Not ready " + str(tt))
        local_logger.info(f"Not ready {str(tt)}")

    # Показать только текст из результатов распознавания.
    local_logger.info("Text chunks:")
    save_json_recognition(req, body['audio']['uri'].split('/')[-2:-1], os.path.splitext(body['audio']['uri'].split('/')[-1])[0])
    save_text_recognition(req, body['audio']['uri'].split('/')[-2:-1], os.path.splitext(body['audio']['uri'].split('/')[-1])[0])
    try:
        for chunk in req['response']['chunks']:
            print(chunk['alternatives'][0]['text'])
    except Exception as e:
        print(e)


def read_file(file):
    with open(file, 'r', encoding='utf-8') as read_file:
        data = read_file.read()
    return data


def similarity(file1, file2, path, filename, ratio_list):
    data1 = read_file(file1)
    data2 = read_file(file2)

    txt1 = data1.splitlines()
    txt2 = data2.splitlines()

    matcher = difflib.SequenceMatcher(None, txt1, txt2)
    d = difflib .Differ()
    diff = d.compare(txt1, txt2)
    ratio_list.append({"filename": filename,
                       "ratio": matcher.ratio()})
    with open(os.getenv('RECOGNITION_FILES_PATH') + str(path) + '/' + f'{filename}_diff.txt', 'w',
                  encoding='utf-8') as file:
        file.writelines('\n'.join(diff))
        file.writelines('\n' + str(matcher.ratio()))
    return matcher.ratio()


if __name__ == '__main__':
    unzip()
    body_list = save_body_list()
    print(body_list)
    print('++++')
    print(body_list[0])
    print('----')
    temp = ' '.join(map(str, body_list[0]['audio']['uri'].split('/')[-2:-1]))
    temp1 = (body_list[0]['audio']['uri'].split('/')[-2:-1])
    print(temp)
    print('~~~~~~~~')
    if not os.path.exists(os.getenv('RECOGNITION_FILES_PATH')):
        os.mkdir(os.getenv('RECOGNITION_FILES_PATH'))
    path = body_list[0]['audio']['uri'].split('/')[-2:-1]
    """    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=os.getenv('endpoint'),
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        region_name='ru-central1'
    )
    local_logger.info(f"Connect to session: {s3}")
    for body in body_list:
        upload_to_bucket(s3,
                         body['audio']['uri'].split('/')[-1],
                         os.path.splitext(body['audio']['uri'].split('/')[-2])[0])
        post_request(body)
    for key in s3.list_objects(Bucket=os.getenv('bucket_name'))['Contents']:
        print(key['Key'])"""
    ratio_list = list()
    for body in body_list:
        body_str = ' '.join(map(str, body['audio']['uri'].split('/')[-2:-1]))
        filename_ = body['audio']['uri'].split('-')[-1]
        filename = filename_.split('/')[0]
        data = file_to_json_data(os.getenv('RECOGNITION_FILES_PATH')
                                 + body_str
                                 + '/'
                                 + os.path.splitext(body['audio']['uri'].split('/')[-1])[0]
                                 + '.json')
        file_yandex = parse_json_yandex(data,
                          body_str,
                          os.path.splitext(body['audio']['uri'].split('/')[-1])[0])
        data_vosk = file_to_json_data(os.getenv('RECOGNITION_FILES_PATH')
                                      + body_str
                                      + '/'
                                      + f"metadata-1-{filename}"
                                      + '.json')
        vosk_list = list()
        for segment in data_vosk['segments']:
            for pair in segment['pairAbonents']:
                file_vosk = parse_json_vosk(pair, body_str)
                vosk_list.append(file_vosk)
        for mas_vosk in vosk_list:
            if file_yandex and mas_vosk:
                if file_yandex.split('.txt')[0] == mas_vosk.split('_vosk.txt')[0]:
                    similarity(file_yandex,
                               mas_vosk,
                               body_str,
                               os.path.splitext(body['audio']['uri'].split('/')[-1])[0],
                               ratio_list)

    print(ratio_list)
    print(len(ratio_list))
    summa_ratio = sum(item['ratio'] for item in ratio_list)
    print(summa_ratio/len(ratio_list))
    with open('stats.txt', 'w', encoding='utf-8') as file:
        file.writelines('Статистика сравнения yandex и vosk\n')
        file.writelines('Где T - общее число элементов в обеих последовательностях,\n'
                        'а M - число совпадений. Мера подобия последовательностей это 2.0*M/T.\n'
                        'Обратите внимание, что мера подобия равная 1.0 будет,\n'
                        'если последовательности идентичны, и 0.0, если они не имеют ничего общего.\n')
        file.writelines(f'Количество выборки: {len(ratio_list)}\n')
        file.writelines(f'Средняя мера подобия: {summa_ratio/len(ratio_list)}\n')
        for ratio in ratio_list:
            file.writelines(str(ratio) + '\n')


