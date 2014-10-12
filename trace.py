import sys
import re
import numpy
import matplotlib.pyplot as plt
import json as js

from datetime import datetime, date, time, timedelta
from itertools import ifilter

# TODO(nnielsen): framework ids and task ids can be anonymized.
# TODO(nnielsen): Generalize cpus / mem into resources object.
# TODO(nnielsen): Add support for reservations.

class trace:
    def __init__(self):
        self.cluster = cluster()

        # Compile regular expressions up front.
        self.re_is_task_status = re.compile('.*Status update.*')
        self.re_is_launch = re.compile('.*Launching task.*')

        # TODO(nnielsen): Standardize UUID parsing.
        self.filter_task = re.compile("I\d+ (.*) \d+ master\.cpp.* Status update (\w+) .* for task (.*) of framework (.*) from slave.*")
        self.filter_launch = re.compile("I\d+ (.*) \d+ master\.cpp.* Launching task (.*) of framework ([0-9\-]+).* with resources (.*) on slave.*")

        self.filter_cpus = re.compile(".*cpus\(\*\):([0-9\.]+).*")
        self.filter_mem = re.compile(".*mem\(\*\):(\d+).*")

        self.last_timestamp = None
        self.day = 0

    def parse(self, f):
        # Will be applied to filter **all** log lines
        def is_task_status_or_launch(s):
            is_task = self.re_is_task_status.match(s) is not None
            is_launch = self.re_is_launch.match(s) is not None
            # TODO(nnielsen): is_slave_attach
            # TODO(nnielsen): is_slave_detach
            # TODO(nnielsen): is_framework_attach
            # TODO(nnielsen): is_framework_detach
            return is_task or is_launch

        filtered_list = ifilter(is_task_status_or_launch, open(f))
        for line in filtered_list:
          self.filter_line(line)

    def adjust_day(self, current):
        if self.last_timestamp is None:
            self.last_timestamp = current
            return
        if self.last_timestamp > current:
            self.day += 1
        self.last_timestamp = current

        return current + timedelta(days=self.day)

    def filter_line(self, line):
        def parse_timestamp(s):
            return datetime.strptime(s, "%H:%M:%S.%f")


        launch_m = self.filter_launch.match(line)
        if launch_m is not None:
            timestamp = parse_timestamp(launch_m.group(1))
            task_id = launch_m.group(2)
            framework_id = launch_m.group(3)
            resources = launch_m.group(4)
            cpus = 0.0
            mem = 0

            timestamp = self.adjust_day(timestamp)

            cpus_m = self.filter_cpus.match(resources)
            if cpus_m is not None:
                cpus = float(cpus_m.group(1))
            else:
                print "Could not parse cpus in %s" % resources

            mem_m = self.filter_mem.match(resources)
            if mem_m is not None:
                mem = int(mem_m.group(1))
            else:
                print "Could not parse mem in %s" % resources

            self.cluster.add_task(framework_id, task_id, cpus, mem)

            return

        task_m = self.filter_task.match(line)
        if task_m is not None:
            timestamp = parse_timestamp(task_m.group(1))
            status = task_m.group(2)
            task_id = task_m.group(3)
            framework_id = task_m.group(4)

            timestamp = self.adjust_day(timestamp)

            self.cluster.handle_update(timestamp, framework_id, task_id, status)

            return

        # TODO(nnielsen): slave_attach_m = filter_slave_attach
        # TODO(nnielsen): slave_detach_m = filter_slave_detach

    def write(self, output):
        #
        # Write JSON to output file.
        #
        f = open(output, 'w')
        cluster_json = self.cluster.json()
        f.write(js.dumps(cluster_json))
        f.close()

class cluster:
    def __init__(self):
        self.frameworks = {}

    def add_slave(self, slave_id, resources):
        pass

    def remove_slave(self, slave_id):
        pass

    def add_task(self, framework_id, task_id, cpus, mem):
        if framework_id not in self.frameworks:
            self.frameworks[framework_id] = framework(framework_id)

        f = self.frameworks[framework_id]
        f.add_task(task_id, cpus, mem)

    def handle_update(self, timestamp, framework_id, task_id, update):
        if framework_id not in self.frameworks:
            return
        f = self.frameworks[framework_id]
        f.handle_update(timestamp, task_id, update)

    def json(self):
        tasks = []
        for framework_id in self.frameworks:
            f = self.frameworks[framework_id]
            ts = f.json()
            tasks.extend(ts)

        out = {}
        out["tasks"] = tasks

        return out

class framework:
    def __init__(self, framework_id):
        self.framework_id = framework_id
        self.tasks = {}

    def add_task(self, task_id, cpus, mem):
        if task_id not in self.tasks:
            self.tasks[task_id] = task(self.framework_id, task_id, cpus, mem)
        else:
            print "Warning: duplicate launch of task '%s' of framework '%s'" % (task_id, framework_id)

    def handle_update(self, timestamp, task_id, status):
        if task_id not in self.tasks:
            # print "Warning: ignoring task '%s' because it hasn't been launched yet" % task_id
            return
        self.tasks[task_id].handle_update(timestamp, status)

    def json(self):
        tasks = []
        for task_id in self.tasks:
            task_json = self.tasks[task_id].json()
            if task_json is not None:
                tasks.append(task_json)
        # TODO(nnielsen): Return structured data with framework info (attached, detached, ...)
        return tasks

class task:
    def __init__(self, framework_id, task_id, cpus, mem):
        self.cpus = cpus
        self.end_time = None
        self.mem = mem
        self.start_time = None
        self.status = None
        self.framework_id = framework_id
        self.task_id = task_id
        self.terminate_status = None

    def handle_update(self, timestamp, status):
        if status == "TASK_RUNNING":
            # Verify first seen RUNNING
            if self.status != "TASK_RUNNING":
                self.status = status
                self.start_time = timestamp
        elif (status == "TASK_FINISHED") or (status == "TASK_FAILED") or (status == "TASK_LOST"):
            if self.terminate_status is None:
                self.terminate_status = status
                self.end_time = timestamp

    def json(self):
        out = {}

        #
        # Task was still running, skip.
        #
        if self.terminate_status is None:
            return None

        if self.status is None:
            return None

        out["task_id"] = self.task_id
        out["framework_id"] = self.framework_id
        out["cpus"] = self.cpus
        out["mem"] = self.mem
        out["time_started"] = self.start_time.isoformat()
        out["time_ended"] = self.end_time.isoformat()
        out["terminated"] = self.terminate_status

        return out

def usage():
    print "%s <mesos-master.log>" % sys.argv[0]

def main():
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    t = trace()

    t.parse(sys.argv[1])
    t.write("trace.json")
    sys.exit(0)

if __name__ == "__main__":
    main()
