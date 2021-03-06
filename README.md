# Mesos Cluster Trace

## Instructions

```
$ python trace.py /tmp/mesos/mesos-master.INFO
$ python stats.py trace.json
cpus:            [ mean: 1.500000        stdev: 0.763763        max: 3.000000    min: 1.000000 samples: 12 ]
mem:             [ mean: 960.000000      stdev: 1442.013407     max: 4096.000000 min: 0.000000 samples: 12 ]
duration (sec):  [ mean: 4.952523        stdev: 7.714514        max: 30.047476   min: 1.015626 samples: 12 ]
```

## Output

![cpus](http://cl.ly/image/1y392j302U1M/cpus.png)
![mem](http://cl.ly/image/2x1L1i3P0K04/mem.png)
![cpusvsmem](http://cl.ly/image/2Z3s2I0p3J1d/download/cpus-mem.png)
![duration](http://cl.ly/image/0n1o0f0k0R1w/durations.png)
