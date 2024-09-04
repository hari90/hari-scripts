import argparse
from datetime import datetime, timedelta
import subprocess
from threading import Thread
import time


parser = argparse.ArgumentParser(description="")
parser.add_argument('-ip','--host', help="DB Host IP")
parser.add_argument('-read_ip','--read_host', default="", help="DB Host IP for reads")
parser.add_argument('-U','--user', help="DB User")
parser.add_argument('-d', '--database', help="Database Name")
parser.add_argument('-vlog', '--vlog', help="Log everything")
args = parser.parse_args()

keep_running = True

def log(f, kind, message):
    f.write(message)
    f.write("\n")
    f.flush()
    print("[%s] %s" % (kind, message))

separator = "------------------------"

def RunWorkload(db_connection, reader):
    file_name = "connectivity_writer.out"
    query = ["-f", "./sql/connectivity_update.sql"]
    kind = "Writer"
    if reader:
        kind = "Reader"
        file_name = "connectivity_reader.out"
    query = ["-f", "./sql/connectivity_select.sql"]

    first_10_avg_duration = 0
    max_duration = 0
    i=0
    last_success = datetime.now()
    previous_succeeded = True
    total_outage = 0

    with open(file_name, "a") as f:
        f.write("\n%s\nStarting %s %s %s\n\n" % (separator, args.host, args.user, args.database))
        while keep_running:
            start_time = datetime.now()
            result = subprocess.run(db_connection + query, capture_output=True, text=True)
            end_time = datetime.now()

            extra_logs = ""
            if result.returncode == 0:
                if not previous_succeeded:
                    outage = (start_time - last_success).total_seconds()
                    total_outage += outage
                    extra_logs += " <Recovered %ss>" % (round(outage, 3))
                previous_succeeded = True
                last_success = end_time
            else:
                previous_succeeded = False
                extra_logs += " <Failed last_success: %s>\nError: %s" % (last_success, result.stderr)

            duration = (end_time - start_time).total_seconds()
            if i < 10:
                first_10_avg_duration += duration
            if i == 10:
                first_10_avg_duration /= 10
                extra_logs += " <Avg Duration %ss>" % (round(first_10_avg_duration, 3))
            if i> 10 and duration > first_10_avg_duration*2.0:
                extra_logs += " <Slow query>"
                if previous_succeeded and duration > max_duration:
                    max_duration = duration


            if args.vlog or len(extra_logs) > 0:
                to_print = "start_time: %s, duration: %ss%s" % (start_time, round(duration, 3), extra_logs)
                log(f, kind, to_print)

            time.sleep(0.1)
            i += 1

        log(f, kind, "\nAvg duration: %ss\nMax duration: %ss\nTotal Outage: %ss\n" % (round(first_10_avg_duration, 3), round(max_duration, 3), round(total_outage, 3)))

# Usage: python3 connect.py -ip <writer_ip> -U <user> -d <database_name> -read_ip <reader_ip>

if __name__ == "__main__":

    program = "psql"
    if args.user == "yugabyte":
        program = "/home/ec2-user/yugabyte-client-2.20.3.0/bin/ysqlsh"

    db_connection = [program, "-h", args.host, "-U", args.user ,"-d", args.database]
    # Create table and insert 1000 rows.
    subprocess.run(db_connection + ["-c", "CREATE TABLE IF NOT EXISTS connectivity_test (a int); INSERT INTO connectivity_test SELECT generate_series(1,1000) WHERE NOT EXISTS (SELECT * FROM connectivity_test);"])

    write_thread = Thread(target = RunWorkload, args = (db_connection, False ))

    if len(args.read_host) > 0:
        db_connection = [program, "-h", args.read_host, "-U", args.user ,"-d", args.database]
    read_thread = Thread(target = RunWorkload, args = (db_connection, True ))

    read_thread.start()
    write_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        keep_running = False
        read_thread.join()
        write_thread.join()
        print("Exiting")
        exit(0)