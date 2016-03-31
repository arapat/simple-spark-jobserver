import os
from flask import Flask, send_file
from Constants import OUTPUT_FOLDER

app = Flask(__name__)


@app.route("/output/<filename>")
def get_file(filename):
    send_file(os.path.join(OUTPUT_FOLDER, filename))


if __name__ == "__main__":
    app.run()
