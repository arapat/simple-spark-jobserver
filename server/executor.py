import json
import os
import pickle
import requests
import shutil
import signal
import subprocess
import sys
from datetime import datetime
from time import sleep, localtime, strftime
from Constants import *

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fGMT"

running_queue = []


def show_message(message):
    sys.stdout.write("[%s] " % strftime("%Y-%m-%d %H:%M:%S", localtime()))
    sys.stdout.write(message + "\n")
    sys.stdout.flush()


def get_spark_status(app_name):
    r = requests.get(APP_STATUS_API)
    for t in r.json():
        if t["name"] == app_name:
            return t
    return None


def get_all_files(dir_name):
    return os.listdir(dir_name)


def update_result(app_name, status, detailed=False):
    r = {"appId": app_name, "status": status}
    if detailed:
        r["stdout"] = DOWNLOAD_URL + "/%s-stdout" % app_name
        r["stderr"] = DOWNLOAD_URL + "/%s-stderr" % app_name
    if detailed and status != CE_MESSAGE:
        spark = get_spark_status(app_name)
        if not spark:
            r["status"] = FAILED_TO_LAUNCH_MESSAGE
        else:
            r["runId"] = spark["id"]
            t1 = datetime.strptime(
                spark["attempts"][0]["startTime"], TIME_FORMAT)
            t2 = datetime.strptime(
                spark["attempts"][0]["endTime"], TIME_FORMAT)
            r["duration"] = (t2 - t1).seconds
    json.dump(r, open(os.path.join(RESULT_FOLDER, app_name + ".res"), "wb"))
    show_message("Result file for app %s (%s) is updated." %
                 (app_name, r["status"]))


def save_streams(app_name, process):
    open(os.path.join(OUTPUT_FOLDER, app_name + "-stdout"), "w").write(
        process.stdout.read())
    open(os.path.join(OUTPUT_FOLDER, app_name + "-stderr"), "w").write(
        process.stderr.read())


def run_program():
    progs = get_all_files(UPLOAD_FOLDER)
    if not progs:
        return

    # Retrive next file to run
    filename = sorted(progs)[0]
    app_name = filename.split('.', 1)[0]
    src_path = os.path.join(UPLOAD_FOLDER, filename)
    rt_path = os.path.join(RUNTIME_FOLDER, filename)
    shutil.move(src_path, rt_path)

    # Compile file
    show_message("Compiling a new file %s." % app_name)
    update_result(app_name, COMPILE_MESSAGE)
    process = subprocess.Popen((COMPILE_COMMAND % (rt_path)).split(),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while process.poll() is None:
        sleep(COMPILE_INTERVAL)
    if process.poll() != 0:
        save_streams(app_name, process)
        update_result(app_name, CE_MESSAGE, detailed=True)
        return

    # Submit file to Spark cluster
    show_message("Submitting a new file %s." % app_name)
    update_result(app_name, RUNNING_MESSAGE)
    start = datetime.now()
    process = subprocess.Popen((SUBMIT_COMMAND % (app_name, rt_path)).split(),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    running_queue.append((app_name, process, start))
    show_message("New app %s is submitted." % app_name)


def refresh():
    def remove(k):
        app_name, process, start = running_queue[k]
        filename = app_name + ".py"
        shutil.move(os.path.join(RUNTIME_FOLDER, filename),
                    os.path.join(ARCHIVE_FOLDER, filename))
        del running_queue[k]
        show_message("Job %s is removed." % app_name)

    k = 0
    now = datetime.now()
    while k < len(running_queue):
        app_name, process, start = running_queue[k]
        if process.poll() is None and (now - start).seconds > TIMEOUT:
            os.kill(process.pid, signal.SIGKILL)
            save_streams(app_name, process)
            update_result(app_name, TLE_MESSAGE, detailed=True)
            remove(k)
        elif process.poll() is not None:
            save_streams(app_name, process)
            if process.poll() == 0:
                update_result(app_name, COMPLETED_MESSAGE, detailed=True)
            else:
                update_result(app_name, RTE_MESSAGE, detailed=True)
            remove(k)
        else:
            k = k + 1
    if len(running_queue) < QUEUE_SIZE:
        run_program()


if __name__ == "__main__":
    while True:
        show_message("Refreshing...")
        refresh()
        show_message("Refreshing finished.")
        sleep(REFRESH_INTERVAL)
