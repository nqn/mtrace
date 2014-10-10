import sys
import re
import numpy
import matplotlib.pyplot as plt

from datetime import datetime, date, time
from itertools import ifilter

# TODO(nnielsen): Wire up task per framework to avoid collision.
class framework:
    def __init__(self, framework_id):
        self.framework_id = framework_id
        self.tasks = {}

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

    print "%s [ mean: %f\t stdev: %f\tmax: %f\tmin: %f samples: %d ]" % (title, mean, stdev, maximum, minimum, len(data_points))

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

    frameworks = {}

    filter_task = re.compile("I\d+ (.*) \d+ master\.cpp.* Status update (\w+) .* for task (.*) of framework (.*) from slave.*")
    filter_launch = re.compile("I\d+ (.*) \d+ master\.cpp.* Launching task (.*) of framework ([0-9\-]+) .* with resources (.*) on slave.*")
    filter_resources = re.compile("cpus\(\*\):([0-9\.]+); mem\(\*\):(\d+).*")
    for line in filtered_list:
        task_m = filter_task.match(line)
        if task_m is not None:
            timestamp = datetime.strptime(task_m.group(1), "%H:%M:%S.%f")
            status = task_m.group(2)
            task_id = task_m.group(3)
            framework_id = task_m.group(4)

            if framework_id not in frameworks:
                continue

            f = frameworks[framework_id]
            if task_id not in f.tasks:
                continue

            if (status == "TASK_RUNNING"):
                f.tasks[task_id].started(status, timestamp)
            elif (status == "TASK_FINISHED") or (status == "TASK_FAILED") or (status == "TASK_LOST"):
                f.tasks[task_id].terminated(status, timestamp)
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

                if framework_id not in frameworks:
                    frameworks[framework_id] = framework(framework_id)
                f = frameworks[framework_id]

                f.tasks[task_id] = task(task_id, cpus, mem)

    durations = []
    cpus_list = []
    mem_list = []
    for framework_id in frameworks:
        f = frameworks[framework_id]
        for task_id in f.tasks:
            t = f.tasks[task_id]
            d = t.duration()
            if d is not None:
              durations.append(d.total_seconds())
            cpus_list.append(t.cpus)
            mem_list.append(t.mem)

    stat("duration:\t", durations)
    stat("cpus:\t\t", cpus_list)
    stat("mem:\t\t", mem_list)

    plt.subplot(2, 1, 1)
    num_bins = 50
    n, bins, patches = plt.hist(durations, num_bins, normed=0, facecolor='green', alpha=0.5)
    plt.xlabel('Duration (seconds)')
    plt.ylabel('Frequency')
    plt.title('Task duration')

    plt.subplot(2, 1, 2)
    plt.scatter(cpus_list, mem_list)
    plt.xlabel('CPUs')
    plt.ylabel('Memory (MB)')

    plt.subplots_adjust(left=0.15)
    plt.savefig('trace.png')


if __name__ == "__main__":
    main()
