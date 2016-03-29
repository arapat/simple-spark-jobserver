import json
import os
import pickle
import requests
import shutil
from time import sleep
from Constants import (DB_PATH, UPLOAD_FOLDER, ARCHIVE_FOLDER, RESULT_FOLDER,
                       OUTPUT_FOLDER)
from Constants import APP_STATUS_API, SUBMIT_COMMAND

db = None
to_file_id = None


def load_db():
    global db, to_file_id
    db = pickle.load(open(DB_PATH, "rb"))
    to_file_id = dict([(b, a) for a, b in db["runs"]])


def dump_db():
    pickle.dump(db, open(DB_PATH, "wb"))


def get_app_status():
    r = requests.get(APP_STATUS_API)
    return r.json()


def get_all_files(dir_name):
    return os.listdir(dir_name)


def update_result(result, app_name):
    spark_id = result['id']
    if (app_name, spark_id) not in db["runs"]:
        to_file_id[spark_id] = app_name
        db["runs"].append((app_name, spark_id))
        dump_db()
    r = {
        "appId": app_name,
        "runId": spark_id,
        "name": result["name"],
        "startTime": result['attempts'][0]['startTime'],
        "endTime": result['attempts'][0]['endTime']
    }
    if result["attempts"][0]["completed"]:
        r['stdout'] = open(
            os.path.join(OUTPUT_FOLDER, app_name + "-stdout.txt")).read()
        r['stderr'] = open(
            os.path.join(OUTPUT_FOLDER, app_name + "-stderr.txt")).read()
    else:
        r["status"] = "running"
    json.dump(r, open(os.path.join(RESULT_FOLDER, app_name + ".res"), "wb"))


def run_program(queue_length):
    progs = get_all_files(UPLOAD_FOLDER)
    if not progs:
        return

    filename = sorted(progs)[0]
    app_name = filename.split(',', 1)[0]
    src_path = os.path.join(UPLOAD_FOLDER, filename)
    stdout_path = os.path.join(OUTPUT_FOLDER, app_name + "-stdout.txt")
    stderr_path = os.path.join(OUTPUT_FOLDER, app_name + "-stderr.txt")
    os.system(SUBMIT_COMMAND % (src_path, stdout_path, stderr_path))

    # Wait until the Spark server received the program
    status = get_app_status()
    while len(status) <= queue_length:
        sleep(5)
        status = get_app_status()

    update_result(status[0], appName)
    tgt_path = os.path.join(ARCHIVE_FOLDER, filename)
    shutil.move(src_path, tgt_path)


def refresh():
    status = get_app_status()
    if status[0]["attempts"][0]["completed"]:
        spark_id = status[0]["id"]
        update_result(status[0], to_file_id[spark_id])
        run_program(len(status))


if __name__ == "__main__":
    load_db()
    while True:
        refresh()
        sleep(15)
