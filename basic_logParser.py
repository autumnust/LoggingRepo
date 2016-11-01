'''
The parser result should contains: 
- How many stage does the whole application have ? 
- In each stage, how many seconds does each operation occupied? 
- In each stage, how many parititon 
'''

import sys, getopt
from collections import defaultdict
from datetime import datetime, timedelta

filename = "../Desktop/result.log" 
output = "output"
old_stage_id = 0
op_info = defaultdict(list)
# nested defaultdict 
stage_info = defaultdict(lambda: defaultdict(list))

def nano2Readable(ns): 
	return timedelta(microseconds=round(ns, -3) // 1000)


def output_stage_info(stage_id, stage_info, output): 
	print "printing..."
	num_of_par = len(stage_info) 
	output.write( "Number of Partitions(tasks):" + str(num_of_par) + " in stage Id: " + str(stage_id) +  "\n" )	
	for par_id,op_info in stage_info.iteritems():
		output.write("Partition " + str(par_id) + "\n")
		for op_name, time_interval in op_info.iteritems(): 
			output.write("\t" + op_name + ":" +str(nano2Readable(time_interval[0])) + "  ----->  " \
				+ str(nano2Readable(time_interval[1]))+ ", lasting:" + str( nano2Readable(time_interval[1] - time_interval[0])) + "\n")
	output.write("\n\n")


with open (output, 'w') as f : 
	with open(filename, 'r') as fd : 
		for line in fd: 
			# First 33 characters, just strip it. 
			line = line[34:]
			[rdd_type, task_attmpt_id, par_id, stage_id] = line.split(",")
			stage_id = int(stage_id.split(":")[1])
			par_id = int(par_id[(par_id.find(":")+1):]) 		
			task_attmpt_id = int(task_attmpt_id[(task_attmpt_id.find(":")+1):])
			op_name = rdd_type.split("]")[0]
			# # End or start is not important, just make sure each operation's time interval has two elements. 
			timestamp = long(rdd_type.split("]")[1].split(":")[1])

			if (old_stage_id != stage_id): 
				# write the things into file. 
				output_stage_info(old_stage_id, stage_info, f)
				old_stage_id = stage_id 
				stage_info.clear()
				stage_info[par_id][op_name].append(timestamp)
			else: 
				stage_info[par_id][op_name].append(timestamp)

	output_stage_info(old_stage_id, stage_info, f)
	fd.close()
f.close()