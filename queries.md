# Prometheus queries
## Spark
```
http://localhost:9090/graph?g0.expr=metrics_CLIWOC_driver_jvm_total_used_Number&g0.tab=0&g0.stacked=0&g0.range_input=20s&g0.end_input=2021-04-17%2008%3A26%3A10&g0.moment_input=2021-04-17%2008%3A26%3A10&g1.expr=metrics_CLIWOC_driver_appStatus_stages_completedStages_Count&g1.tab=0&g1.stacked=0&g1.range_input=20s&g1.end_input=2021-04-17%2008%3A26%3A10&g1.moment_input=2021-04-17%2008%3A26%3A10&g2.expr=avg%20by%20(instance)%20(rate(node_cpu_seconds_total%7Bjob%3D%22node%22%2Cmode%3D%22iowait%22%7D%5B20s%5D)%20%20*100%20)&g2.tab=0&g2.stacked=0&g2.range_input=20s&g2.end_input=2021-03-27%2007%3A38%3A00&g2.moment_input=2021-03-27%2007%3A38%3A00&g3.expr=100%20*%20(1%20-%20((avg_over_time(node_memory_MemFree_bytes%5B20s%5D)%20%2B%20avg_over_time(node_memory_Cached_bytes%5B20s%5D)%20%2B%20avg_over_time(node_memory_Buffers_bytes%5B20s%5D))%20%2F%20avg_over_time(node_memory_MemTotal_bytes%5B20s%5D)))&g3.tab=0&g3.stacked=0&g3.range_input=20s&g3.end_input=2021-03-27%2007%3A37%3A59&g3.moment_input=2021-03-27%2007%3A37%3A59&g4.expr=node_memory_MemTotal_bytes%20-%20node_memory_MemFree_bytes%20-%20node_memory_Cached_bytes%20-%20node_memory_Buffers_bytes&g4.tab=0&g4.stacked=0&g4.range_input=30s&g4.end_input=2021-04-17%2008%3A26%3A10&g4.moment_input=2021-04-17%2008%3A26%3A10
```

```
http://localhost:9090/graph?g0.expr=metrics_CLIWOC_driver_jvm_total_used_Number&g0.tab=0&g0.stacked=0&g0.range_input=20s&g0.end_input=2021-03-27%2007%3A38%3A00&g0.moment_input=2021-03-27%2007%3A38%3A00&g1.expr=100%20-%20(avg%20by%20(instance)%20(rate(node_cpu_seconds_total%7Bjob%3D%22node%22%2Cmode%3D%22idle%22%7D%5B20s%5D))%20*%20100)&g1.tab=0&g1.stacked=0&g1.range_input=20s&g1.end_input=2021-03-27%2007%3A38%3A00&g1.moment_input=2021-03-27%2007%3A38%3A00&g2.expr=avg%20by%20(instance)%20(rate(node_cpu_seconds_total%7Bjob%3D%22node%22%2Cmode%3D%22iowait%22%7D%5B20s%5D)%20%20*100%20)&g2.tab=0&g2.stacked=0&g2.range_input=20s&g2.end_input=2021-03-27%2007%3A38%3A00&g2.moment_input=2021-03-27%2007%3A38%3A00&g3.expr=100%20*%20(1%20-%20((avg_over_time(node_memory_MemFree_bytes%5B20s%5D)%20%2B%20avg_over_time(node_memory_Cached_bytes%5B20s%5D)%20%2B%20avg_over_time(node_memory_Buffers_bytes%5B20s%5D))%20%2F%20avg_over_time(node_memory_MemTotal_bytes%5B20s%5D)))&g3.tab=0&g3.stacked=0&g3.range_input=20s&g3.end_input=2021-03-27%2007%3A37%3A59&g3.moment_input=2021-03-27%2007%3A37%3A59&g4.expr=node_memory_MemTotal_bytes%20-%20node_memory_MemFree_bytes%20-%20node_memory_Cached_bytes%20-%20node_memory_Buffers_bytes&g4.tab=0&g4.stacked=0&g4.range_input=20s&g4.end_input=2021-03-27%2007%3A38%3A00&g4.moment_input=2021-03-27%2007%3A38%3A00
```
### Stages
```
metrics_CLIWOC_driver_appStatus_stages_completedStages_Count
```
### JVM
```
metrics_CLIWOC_driver_jvm_total_used_Value
```

## CPU

### Util
```
100 - (avg by (instance) (rate(node_cpu_seconds_total{job="node",mode="idle"}[20s])) * 100)
```
### iowait
```
avg by (instance) (rate(node_cpu_seconds_total{job="node",mode="iowait"}[20s])*100 )
```

## Mem util
```
100 * (1 - ((avg_over_time(node_memory_MemFree_bytes[20s]) + avg_over_time(node_memory_Cached_bytes[20s]) + avg_over_time(node_memory_Buffers_bytes[20s])) / avg_over_time(node_memory_MemTotal_bytes[20s])))
```

```
node_memory_MemTotal_bytes[20s] - node_memory_MemFree_bytes[20s] - node_memory_Cached_bytes - node_memory_Buffers_bytes
```
