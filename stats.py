# Stats:
# TODO(nnielsen): Resource utilization (cluster capacity) per update
# TODO(nnielsen): Task resource distribution (cpus, mem)
# TODO(nnielsen): Task duration distribution
# TODO(nnielsen): Support stacked bars for per-framework stats.
# TODO(nnielsen): Show parallel tasks over time.

import json as js
import numpy
import sys
import matplotlib.pyplot as plt

from datetime import datetime, date, time, timedelta

class stats:
    def __init__(self):
        self.cpus = []
        self.mem = []
        self.durations = []
        pass

    def parse(self, filename):
        f = open(filename, 'r')
        json = f.read()
        trace = js.loads(json)

        def parse_timestamp(s):
           return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f") 
            

        if 'tasks' in trace:
            tasks = trace['tasks']
            for t in tasks:
                self.cpus.append(float(t["cpus"]))
                self.mem.append(int(t["mem"]))
                self.durations.append((parse_timestamp(t["time_ended"]) - parse_timestamp(t["time_started"])).total_seconds())

        f.close()
        
        self.log("cpus: ", self.cpus)
        self.log("mem: ", self.mem)
        self.log("duration (sec): ", self.durations)

        self.hist(None, "Cores", "Frequency", self.cpus, "cpus.png")
        self.hist(None, "Memory (MB)", "Frequency", self.mem, "mem.png")
        self.hist(None, "Duration (seconds)", "Frequency", self.durations, "durations.png")
        self.scatter("Cores vs Memory", "Cores", "Memory (MB)", self.cpus, self.mem, "cpus-mem.png")

    def hist(self, title, xlabel, ylabel, data_points, output):
        num_bins = 50
        n, bins, patches = plt.hist(data_points, num_bins)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        if title is not None:
            plt.title(title)
        plt.savefig(output)
        plt.clf()

    def scatter(self, title, xlabel, ylabel, xdata, ydata, output):
        plt.scatter(xdata, ydata)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        if title is not None:
            plt.title(title)
        plt.savefig(output)
        plt.clf()
    
    def log(self, title, data_points):
        # TODO(nnielsen): compute percentile.
        mean = numpy.mean(data_points)
        stdev = numpy.std(data_points)
        maximum = max(data_points)
        minimum = min(data_points)
    
        print "%s [ mean: %f\t stdev: %f\tmax: %f\tmin: %f samples: %d ]" % (title, mean, stdev, maximum, minimum, len(data_points))

def usage():
    print "%s <trace.json>" % sys.argv[0]

def main():
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    s = stats()
    s.parse(sys.argv[1])
    sys.exit(0)

if __name__ == "__main__":
    main()
