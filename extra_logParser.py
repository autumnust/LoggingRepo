'''
The parser result should contains: 
- In each stage, how many partitions? 
- In each RDD, how partitiones ? 
- Between two RDDs in a Stage, how much each parititon spent [The maximal one is the link time]
'''
import datetime
import sys, getopt
from collections import defaultdict
from datetime import datetime, timedelta

filename = "" 
output = "output"
old_stage_id = 0
op_info = defaultdict(list)

# nested defaultdict 
#[stage_id, [rdd_id, [part_id, [starTime, endTime]]]]
stage_info = defaultdict(lambda: defaultdict(lambda : defaultdict(list)))
#[rdd_id, rddName]
rdd_id_name_map = defaultdict(str)
#[stage_id, stageName]
stage_id_name_map = defaultdict(str)

# Given the time in ms, return the string that is readable. 
def milli2Readable(target_date_time_ms): 
	base_datetime = datetime.datetime( 1970, 1, 1)
	delta = datetime.timedelta( 0, 0, 0, target_date_time_ms )
	target_date = base_datetime + delta
	return target_date 	

# Return the longest time among all the partitions within a RDD. 
# In the format of [stragglerid, start_time, end_time]
def getSlowestPartitionTime(part_list): 
	time_diff = 0
	straggler_id = 0 
	start_time = 0 
	end_time = 0 
	for part_id, time_interval in part_list.iteritems(): 
		tmp_time_diff = time_interval[1] - time_interval[0]
		if (tmp_time_diff > time_diff ): 
			straggler_id = par_id 			
			time_diff = tmp_time_diff 			
			start_time =  time_interval[0] 
			end_time =  time_interval[1]
	return [straggler_id, starTime, end_time ]
''' 
Given the Stage Id, output the parsing result of a stage in the following way: 
Stage Id (Number of Parititons):
	- RDD_1 (num of partitions)
		Lasting: xxxx time
	- RDD_2 (num of parititons)
		Lasting: xxxx time
	...
	
	- RDD_n (num of partitions)
'''
# TODO: A verbose mode to output each partition's time for that transformation. 
def output_stage_info(stage_info, stage_id, output): 
	rdd_list = stage_info[stage_id]
	output.write("Stage Id(" + str(len(rdd_list)) + ")" + "\n")

	for rdd_id, part_list in rdd_list.iteritems(): 
		output.write("\t - RDD_" + str(rdd_id) + "(" + str(len(part_list)) + ")\n")
		[straggler_id, start_time, end_time] = getSlowestPartitionTime(part_list)
		output.write("\t\t Start:" + str(milli2Readable(start_time)) + " End: " + str(milli2Readable(end_time)) \
					+ "  Lasting: " + (end_time - start_time ) + "ms" + "\n"
					) 		
	output.write("\n\n")

	
# Main program
with open (output, 'w') as f : 
	with open(filename, 'r') as fd : 
		for line in fd: 
			# ***  Parsing  ***
			# First 36 characters, just strip it. 
			line = line[37:]
			split_result = line.split(",") 
			# task_type can be: ShuffleMapTask, ResultTask, iterator.FromParent, iterator.FromCache
			if (len(split_result) == 4 ):
				[task_type, task_attmpt_id, par_id, stage_id] = split_result
			elif ( len(split_result) == 6): 
				[task_type, rdd_id, rdd_name, task_attmpt_id, par_id, stage_id] = split_result			

			stage_id = int(stage_id.split(":")[1])
			par_id = int(par_id[(par_id.find(":")+1):]) 		
			task_attmpt_id = int(task_attmpt_id[(task_attmpt_id.find(":")+1):])
			task_name = task_type.split("]")[0]
			# End or start is not important, just make sure each operation's time interval has two elements. 
			timestamp = long(rdd_type.split("]")[1].split(":")[1])

			# ***  Info Assemble  ***
			stage_info[stage_id][rdd_id][par_id].append(timestamp)			
	
	# Output all the stage. 
	for i in xrange(len(stage_info)):
		output_stage_info(stage_info, i, f)
	fd.close()
f.close()