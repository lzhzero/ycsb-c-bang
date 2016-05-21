# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

# keymaxvalue: means the max key that exist in the database
# snapshot means whether to run snapshot transactions or normal ones
#
#

recordcount=0
operationcount=800000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0
readmodifywriteproportion=0

keymaxvalue=10000000
snapshot=1

requestdistribution=uniform

