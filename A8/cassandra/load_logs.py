import sys
import os
import gzip
import re
import uuid
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime


def parse_log(line):
    line_re = re.compile(
        r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    parsed = line_re.match(line)
    if parsed:
        return {
            "host_name": parsed.group(1),
            "datetime": datetime.strptime(parsed.group(2), "%d/%b/%Y:%H:%M:%S"),
            "path": parsed.group(3),
            "bytes": int(parsed.group(4))
        }
    return None


def main(input, keyspace, table):
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)

    insert_log = session.prepare(
        f"INSERT INTO {table} (id, host, datetime, path, bytes) VALUES (?, ?, ?, ?, ?)")

    for f in os.listdir(input):
        with gzip.open(os.path.join(input, f), 'rt', encoding='utf-8') as logfile:
            batch = BatchStatement()
            batch_size = 0

            for line in logfile:
                log = parse_log(line)
                if not log:
                    continue
                host = log['host_name']
                datetime = log['datetime']
                path = log['path']
                bytes = log['bytes']
                id = uuid.uuid4()

                batch.add(insert_log, (id, host, datetime, path, bytes))
                batch_size += 1

                if batch_size == 300:
                    session.execute(batch)
                    batch = BatchStatement()
                    batch_size = 0

            if batch_size > 0:
                session.execute(batch)
                batch = BatchStatement()
                batch_size = 0

    cluster.shutdown()


if __name__ == "__main__":
    input = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(input, keyspace, table)
