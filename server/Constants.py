
# For storing permanent info
DB_PATH = "/path/to/the/db"
# Where users upload programs
UPLOAD_FOLDER = "/path/to/the/uploads"
# For storing running/completed programs
ARCHIVE_FOLDER = "/path/to/the/archive"
# For directing standard output and standard error
OUTPUT_FOLDER = "/path/to/the/output"
# Where we store all information about programs
RESULT_FOLDER = "/path/to/the/results"
ALLOWED_EXTENSIONS = set(["py"])

SPARK_PATH = "/path/to/spark"
MASTER_IP = "<master_ip>"
MASTER_URL = "spark://%s:7077" % MASTER_IP
SUBMIT_COMMAND = (SPARK_PATH + "/bin/spark-submit --master " + MASTER_URL +
                  " %s 1> %s 2> %s & disown")

SPARK_SERVER = "http://%s:8080/api/v1" % MASTER_IP
APP_STATUS_API = SPARK_SERVER + "/applications"

INIT_DB = {"runs": []}
