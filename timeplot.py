import json as js
import numpy
import sys

import matplotlib.pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter
import datetime

from datetime import datetime, date, time, timedelta

def usage():
    print "%s <trace.json>" % sys.argv[0]

class task:
    def __init__(self):
        pass

class slave:
    def __init__(self):
        pass

class timeplot:
    def __init__(self):
        pass

    def tick(self):
        # Total capacity
        # Total used
        pass

    def parse(self, filename):
        f = open(filename, 'r')
        json = f.read()
        trace = js.loads(json)

        def parse_timestamp(s):
           return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f") 
        
        class diff:
            def __init__(self, timestamp, add, cpus, mem):
                self.timestamp = timestamp
                self.add = add
                self.cpus = cpus
                self.mem = mem
                    
        task_diff = []
        if 'tasks' in trace:
            tasks = trace['tasks']
            for t in tasks:
                cpus = float(t["cpus"])
                mem = int(t["mem"])
                task_diff.append(diff(parse_timestamp(t["time_started"]), True, cpus, mem))
                task_diff.append(diff(parse_timestamp(t["time_ended"]), False, cpus, mem))

        sorted_task_diff = sorted(task_diff, key=lambda td: td.timestamp)


        years = YearLocator()   # every year
        months = MonthLocator()  # every month
        yearsFmt = DateFormatter('%Y')

        task_date = []
        task_cpus_value = []
        task_mem_value = []
        total_task_cpus = 0.0
        total_task_mem = 0
       
        for td in sorted_task_diff:
            if td.add:
                total_task_cpus += td.cpus
                total_task_mem += td.mem
            else:
                total_task_cpus -= td.cpus
                total_task_mem -= td.mem

            task_date.append(td.timestamp)
            task_cpus_value.append(total_task_cpus)
            task_mem_value.append(total_task_mem)

        slave_diff = []
        if 'slaves' in trace:
            slaves = trace['slaves']
            for s in slaves:
                cpus = float(s["cpus"])
                mem = int(s["mem"])
                slave_diff.append(diff(parse_timestamp(s["time_started"]), True, cpus, mem))
                if "time_ended" in s:
                    slave_diff.append(diff(parse_timestamp(s["time_ended"]), False, cpus, mem))
                pass

        sorted_slave_diff = sorted(slave_diff, key=lambda sd: sd.timestamp)

        total_slave_cpus = 0.0
        total_slave_mem = 0
        slave_date = []
        slave_cpus_value = []
        slave_mem_value = []
        for sd in sorted_slave_diff:
            if sd.add:
                total_slave_cpus += sd.cpus
                total_slave_mem += sd.mem
            else:
                total_slave_cpus -= sd.cpus
                total_slave_mem -= sd.mem

            slave_date.append(sd.timestamp)
            slave_cpus_value.append(total_slave_cpus)
            slave_mem_value.append(total_slave_mem)

        fig, ax = plt.subplots()
        ax.plot_date(task_date, task_cpus_value, '-')
        ax.plot_date(slave_date, slave_cpus_value, '-')
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(months)
        ax.autoscale_view()
        ax.fmt_xdata = DateFormatter('%H:%M:%S.%f')
        fig.autofmt_xdate()
        plt.ylabel('Cores')
        plt.savefig('utilization-cpu.png')
        plt.clf()

        fig, ax = plt.subplots()
        ax.plot_date(task_date, task_mem_value, '-')
        ax.plot_date(slave_date, slave_mem_value, '-')
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(months)
        ax.autoscale_view()
        ax.fmt_xdata = DateFormatter('%H:%M:%S.%f')
        fig.autofmt_xdate()
        plt.ylabel('Memory (MB)')
        plt.savefig('utilization-mem.png')
        plt.clf()

        f.close()

def main():
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    t = timeplot()
    t.parse(sys.argv[1])
    sys.exit(0)

if __name__ == "__main__":
    main()
