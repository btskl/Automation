import mysql.connector as sql
from mysql.connector import errorcode
import time
import datetime

# defining the variables for the database
db_host = "DB_host"
db_name = "DB_NAME"
db_user = "DB_USER"
db_pass = "DB_PASS"
db_port = "DB_PORT"

sqlfile = "sqlfile.sql"


# function for opening databse connection
def open_dbconnection():
    # opens the databse connection
    connection = None
    try:
        db = sql.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name,
            port=db_port
        )
        return db
    except sql.Error as error:
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            # If the error is access denied, print the error
            print("Something is wrong with your user name or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            # If the database doesn't exist, print the error
            print("Database does not exist")
            db = sql.connect(
                host=db_host,
                user=db_user,
                password=db_pass,
                port=db_port
            )
            cursor = db.cursor(dictionary=True)

            db_create = "CREATE DATABASE IF NOT EXISTS " + db_name
            cursor.execute(db_create, {'db_name': db_name})
            return sql.connect(
                host=db_host,
                user=db_user,
                password=db_pass,
                database=db_name,
                port=db_port
            )
        else:
            print(error)


# check if the database is created and equipped
def check_db():
    db = open_dbconnection()

    cursor = db.cursor()
    try:
        cursor.execute("SELECT * FROM hosts")
    except:
        file = open(sqlfile, "r").read()

        sqlcommands = file.split(";")

        for command in sqlcommands:
            try:
                cursor.execute(command)
            except sql.Error as error:
                error.errno == errorcode.ER_TABLE_EXISTS_ERROR
                print("Table already exists")


def add_data(host_id, cpu_usage, ram_usage, disk_usage, bytes_sent, bytes_recv, speed, temperature):
    db = open_dbconnection()
    cursor = db.cursor(dictionary=True)
    timestamp = int(time.time())
    query = (
        "INSERT INTO monitoring_data (timestamp, host_id, cpu_usage, ram_usage, disk_usage, bytes_sent, bytes_recv,speed,temperature) VALUES (%s, %s, %s, %s,%s, %s, %s,%s,%s)")

    cursor.execute(query,
                   (timestamp, host_id, cpu_usage, ram_usage, disk_usage, bytes_sent, bytes_recv, speed, temperature))

    db.commit()
    cursor.fetchwarnings()
    db.close()


def get_hosts(id=None, hostname=None, os_type=None, os_version=None, cpu_arch=None):
    db = open_dbconnection()
    cursor = db.cursor(dictionary=True)
    table = "hosts"
    if id:
        query = "SELECT * FROM hosts WHERE id = %(id)s"
    elif os_type:
        query = "SELECT * FROM hosts WHERE os_type = %(os_type)s"
    elif os_version:
        query = "SELECT * FROM hosts WHERE os_version = %(os_version)s"
    elif cpu_arch:
        query = "SELECT * FROM hosts WHERE cpu_arch = %(cpu_arch)s"
    elif hostname:
        query = "SELECT * FROM hosts WHERE hostname = %(hostname)s"
    else:
        query = "SELECT * FROM hosts"

    cursor.execute(query, {'id': id, 'hostname': hostname, 'os_type': os_type,
                           'os_version': os_version, 'cpu_arch': cpu_arch})

    data = cursor.fetchall()
    db.close()

    return data


def add_host(hostname, os_type, os_version, cpu_arch):
    db = open_dbconnection()
    db.raise_on_warnings = True
    table = "hosts"

    cursor = db.cursor()
    query = "INSERT INTO hosts (hostname, os_type, os_version, cpu_arch) VALUES (%s, %s, %s, %s)"

    cursor.execute(query, (hostname, os_type, os_version, cpu_arch))

    db.commit()
    cursor.fetchwarnings()
    db.close()