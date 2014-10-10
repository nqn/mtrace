import sys
import re
import numpy

from datetime import datetime, date, time
from itertools import ifilter

# TODO(nnielsen): Wire up task per framework to avoid collision.
class framework:
    def __init__(self):
        pass

class task:
    def __init__(self, task_id, cpus, mem):
        # TODO(nnielsen): Verify non-terminal status.
        self.task_id = task_id
        self.start_time = None
        self.end_time = None
        self.cpus = cpus
        self.mem = mem

    def started(self, status, start_time):
        self.status = status
        self.start_time = start_time

    def terminated(self, status, end_time):
        # TODO(nnielsen): Verify terminal status.
        self.terminate_status = status
        self.end_time = end_time

    def duration(self):
        if (self.start_time != None) and (self.end_time != None):
            return self.end_time - self.start_time
        return None

def stat(title, data_points):
    mean = numpy.mean(data_points)
    stdev = numpy.std(data_points)
    maximum = max(data_points)
    minimum = min(data_points)

    print "%s [ mean: %f\t stdev: %f\tmax: %f\tmin: %f ]" % (title, mean, stdev, maximum, minimum)

re_is_task_status = re.compile('.*] Status update.*')
re_is_launch = re.compile('.*] Launching task.*')
def is_task_status_or_launch(s):
   is_task = re_is_task_status.match(s) != None
   is_launch = re_is_launch.match(s) != None
   return is_task or is_launch

def usage():
    print "%s <mesos-master.log>" % sys.argv[0]

def main():
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    filtered_list = ifilter(is_task_status_or_launch, open(sys.argv[1]))

    tasks = {}

    filter_task = re.compile("I\d+ (.*)  \d+ master\.cpp.* Status update (\w+) .* for task (.*) of framework (.*)")
    filter_launch = re.compile("I\d+ (.*)  \d+ master\.cpp.* Launching task (.*) of framework (.*) with resources (.*) on slave.*")
    filter_resources = re.compile("cpus\(\*\):([0-9\.]+); mem\(\*\):(\d+).*")
    for line in filtered_list:
        task_m = filter_task.match(line)
        if task_m is not None:
            timestamp = datetime.strptime(task_m.group(1), "%H:%M:%S.%f")
            status = task_m.group(2)
            task_id = task_m.group(3)

            if task_id not in tasks:
                continue

            if (status == "TASK_RUNNING"):
                tasks[task_id].started(status, timestamp)
            elif (status == "TASK_FINISHED") or (status == "TASK_FAILED") or (status == "TASK_LOST"):
                tasks[task_id].terminated(status, timestamp)
        else:
            launch_m = filter_launch.match(line)
            if launch_m is not None:
                timestamp = datetime.strptime(launch_m.group(1), "%H:%M:%S.%f")
                task_id = launch_m.group(2)
                framework_id = launch_m.group(3)
                resources = launch_m.group(4)
                cpus = 0.0
                mem = 0

                resources_m = filter_resources.match(resources)
                if resources_m is not None:
                    cpus = float(resources_m.group(1))
                    mem = int(resources_m.group(2))

                tasks[task_id] = task(task_id, cpus, mem)

    durations = []
    cpus_list = []
    mem_list = []
    for task_id in tasks:
        t = tasks[task_id]
        d = t.duration()
        if d is not None:
          durations.append(d.total_seconds())
        cpus_list.append(t.cpus)
        mem_list.append(t.mem)

    stat("duration:\t", durations)
    stat("cpus:\t\t", cpus_list)
    stat("mem:\t\t", mem_list)


if __name__ == "__main__":
    main()
