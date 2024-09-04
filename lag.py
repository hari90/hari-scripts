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
parser.add_argument('-ysqlsh_path', '--ysqlsh_path', default="ysqlsh", help="ysqlsh path")
args = parser.parse_args()

keep_running = True

def log(f, kind, message):
    f.write(message)
    f.write("\n")
    f.flush()
    print("[%s] %s" % (kind, message))

separator = "------------------------"

def RunWriter(db_connection: list):
    kind = "Writer"
    file_name = "lag_writer.out"
    query = ["-f", "./sql/lag_update.sql"]

    with open(file_name, "a") as f:
        f.write("\n%s\nStarting %s %s %s\n\n" % (separator, args.host, args.user, args.database))
        while keep_running:
            subprocess.run(db_connection + query, capture_output=True, text=True)
            time.sleep(0.1)


def RunReader(db_connection: list):
    kind = "Reader"
    file_name = "lag_reader.out"
    query = ["-t", "-f", "./sql/lag_select.sql"]

    first_10_avg = 0
    i=0
    max_lag=0

    with open(file_name, "a") as f:
        f.write("\n%s\nStarting %s %s %s\n\n" % (separator, args.host, args.user, args.database))
        while keep_running:
            start_time = datetime.now()
            result = subprocess.run(db_connection + query, capture_output=True, text=True)
            extra_logs = ""

            # from stdout extract the line that matches 'NOTICE:  Lag: '
            if len(result.stderr) > 0 and "NOTICE:  Lag: " in result.stderr.splitlines()[-1]:
                lag = float(result.stderr.splitlines()[-1].split(":")[-1].strip())
                if lag < 0:
                    extra_logs += " <Negative lag %s>" % lag
                    lag = 0
            else:
                continue

            if lag > max_lag:
                max_lag = lag

            if i < 10:
                first_10_avg += lag
            elif i == 10:
                first_10_avg /= 10
                extra_logs += " <Avg Duration %sms>" % (round(first_10_avg, 3))
            elif i> 10 and lag > first_10_avg*2.0:
                extra_logs += " <High lag>"


            if args.vlog or len(extra_logs) > 0:
                to_print = "start_time: %s, lag: %ss%s" % (start_time, round(lag, 3), extra_logs)
                log(f, kind, to_print)

            time.sleep(0.1)
            i += 1

        log(f, kind, "\nAvg lag: %sms\nMax lag: %sms\n\n" % (round(first_10_avg, 3), round(max_lag, 3)))

# Usage: python3 lag.py -ip <writer_ip> -U <user> -d <database_name> -read_ip <reader_ip>

if __name__ == "__main__":

    program = "psql"
    if args.user == "yugabyte":
        program = args.ysqlsh_path

    db_connection = [program, "-h", args.host, "-U", args.user ,"-d", args.database]
    # Create table and insert 1000 rows.
    subprocess.run(db_connection + ["-c", "CREATE TABLE IF NOT EXISTS lag_test (a int, t TIMESTAMP); INSERT INTO lag_test SELECT generate_series(1,1000), now() WHERE NOT EXISTS (SELECT * FROM lag_test);"])

    write_thread = Thread(target = RunWriter, args = ([db_connection]))

    if len(args.read_host) > 0:
        db_connection = [program, "-h", args.read_host, "-U", args.user ,"-d", args.database]
    read_thread = Thread(target = RunReader, args = ([db_connection]))

    write_thread.start()
    time.sleep(2)
    read_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        keep_running = False
        read_thread.join()
        write_thread.join()
        print("Exiting")
        exit(0)