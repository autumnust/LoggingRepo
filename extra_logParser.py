'''
The parser result should contains:
- In each stage, how many partitions?
- In each RDD, how partitiones ?
- Between two RDDs in a Stage, how much each parititon spent [The maximal one is the link time]
'''
import datetime
import sys, getopt
from collections import defaultdict

filename = "combined.log"
output = "operation_break"
old_stage_id = 0
op_info = defaultdict(list)

# nested defaultdict
#[stage_id, [rdd_id, [part_id, [starTime, endTime]]]]
stage_info = defaultdict(lambda: defaultdict(lambda : defaultdict(list)))
#[rdd_id, rddName]
rdd_id_name_map = defaultdict(str)
#[stage_id, stageName]
stage_id_name_map = defaultdict(str)
#[stage_id, [part_id, [startTime, endTime]]]
stage_time_map = defaultdict(lambda: defaultdict(list))
#[stage_id, [part_id, [shuffle_starTime, shuffle_endTime]]]
rdd_partition_shuffle_write = defaultdict(lambda: defaultdict(list))

#[stage_id, [part_id, [shuffle_starTime, shuffle_endTime]]]
rdd_partition_shuffle_read = defaultdict(lambda: defaultdict(list))

# stage_single_part_info
# [stage_id, [part_id, [rdd_id, [startTime, endTime]]]]
stage_single_part_info = defaultdict(lambda: defaultdict(lambda : defaultdict(list)))

# Given the time in ms, return the string that is readable.
def milli2Readable(target_date_time_ms):
	target_date_time_ms = long(target_date_time_ms)
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
			straggler_id = part_id
			time_diff = tmp_time_diff
			start_time =  time_interval[0]
			end_time =  time_interval[1]
	return [straggler_id, start_time, end_time ]
'''
Given the Stage Id, output the parsing result of a stage in the following way:
Stage Id (Number of Parititons): Stage Name
	- RDD_1(RDD Name) (num of partitions)
		Lasting: xxxx time
	- RDD_2(RDD Name) (num of parititons)
		Lasting: xxxx time
	...

	- RDD_n(RDD Name) (num of partitions)

Starting from: xxxxx
Ending at: xxxxx
'''
# TODO: A verbose mode to output each partition's time for that transformation.
def output_stage_info(stage_info, stage_id, output, stage_id_name_map, rdd_id_name_map, verbose):
	rdd_list = stage_info[stage_id]
	output.write("Stage_"+ str(stage_id) + "(" + str(len(rdd_list)) + "):" + stage_id_name_map[stage_id] + str(stage_id) + "\n")

	start_time=''
	end_time =''
	for rdd_id, part_list in rdd_list.iteritems():
		rdd_real_name = ''
		if (rdd_id_name_map[rdd_id][0] =='.' ):
			rdd_real_name = "InputData"
		else:
			rdd_real_name = rdd_id_name_map[rdd_id]
		output.write("\t - RDD_" + str(rdd_id) + ":Generating " + rdd_real_name + "(" + str(len(part_list)) + ")\n")
		if ( verbose == 0 ):
			[straggler_id, start_time, end_time] = getSlowestPartitionTime(part_list)
			output.write("\t\t Start:" + str(milli2Readable(start_time)) + " End: " + str(milli2Readable(end_time)) \
						+ "  Lasting: " + str(end_time - start_time ) + "ms" + "\n"
						)
		else:
			for part_id, time_interval in part_list.iteritems():
				output.write("\t\tPartition_" + str(part_id) + " start from :" + str(milli2Readable(time_interval[0]))\
					+  " , end at: " + str(milli2Readable(time_interval[1])) + ", Lasting : " \
					+ str(long(time_interval[1]) - long(time_interval[0])) +  "ms  \n")
		# Print all partitions' time.
	output.write("\n\n")

# Reorder the rdd list according to the DAG result.
def reorder_rdd_list(rdd_list, rdd_id_name_map):
	result_list = defaultdict(list)
	for rdd_id, time_interval in rdd_list.iteritems():
		rdd_real_name = rdd_id_name_map[rdd_id]
		if (rdd_real_name[0]=='S'):
			result_list[0] = [rdd_real_name, time_interval]
		elif ( rdd_real_name[0]=='M'):
			result_list[1] = [rdd_real_name, time_interval]
		elif ( rdd_real_name[0]=='U'):
			result_list[3] = [rdd_real_name, time_interval]
		elif (rdd_real_name[0]=='P' and rdd_real_name[1]=='a' ):
			result_list[5] = [rdd_real_name, time_interval]
		elif ( rdd_real_name[0] == 'P' and rdd_real_name[1]=='y'):
			if ( len(result_list[2]) == 0 ) :
				result_list[2] = [rdd_real_name, time_interval]
			elif ( result_list[2][1][1] > time_interval[1] ):
				result_list[4] = result_list[2]
				result_list[2] = [rdd_real_name, time_interval]
			else:
				result_list[4] = [rdd_real_name, time_interval]
	return result_list

# stage_single_part_info structure : [stage_id, [part_id, [rdd_id, [startTime, endTime]]]]
def output_part_oriented_stage_info(stage_single_part_info, stage_id, output, stage_id_name_map, rdd_id_name_map,\
									rdd_partition_shuffle_write, verbose, stats_output):
	if ( stage_id < 5 ):
		return
	part_list = stage_single_part_info[stage_id]
	part_list_write = rdd_partition_shuffle_write[stage_id]
	output.write("Stage_"+ str(stage_id) + ":" + stage_id_name_map[stage_id] + "\n")
	stats_output.write("Stage_"+ str(stage_id) + ":" + stage_id_name_map[stage_id] + "\n")
	rdd_real_name = ''
	start_time=''
	end_time =''

	max_swt = 0 ;
	sum_swt = 0 ;
	for part_id, rdd_list in part_list.iteritems():
		output.write("\t" + str(part_id) + ":")
		# Enforce the RDD order here.
		result_list = reorder_rdd_list(rdd_list, rdd_id_name_map)
		for i in xrange( len(result_list) ):
			rdd_real_name = result_list[i][0]
			time_interval = result_list[i][1]
			if ( verbose == 0 ):
				output.write( rdd_real_name + "[" + str(milli2Readable(time_interval[0])) + "-" + str(milli2Readable(time_interval[1])) + "] --> ")
			else:
				output.write( rdd_real_name + "[" + str( time_interval[1] - time_interval[0] ) + "] --> ")

			if ( i > 0 ):
				stats_output.write("\t\t" + result_list[i-1][0] + "->" + rdd_real_name + ":" + str( time_interval[1] - result_list[i-1][1][1]) + "ms\n" )
		if (len(part_list_write) == len(part_list)):
			output.write("SWT:" + str(milli2Readable(part_list_write[part_id][0])) + "-" + str(milli2Readable(part_list_write[part_id][1])) )
			# Blocking time related stats
		if (len(part_list_write) == len(part_list)):
			if ( part_list_write[part_id][1] > max_swt ) :
				max_swt = part_list_write[part_id][1]
			sum_swt += part_list_write[part_id][1]
		else :
			if ( result_list[len(result_list)-1][1][1] > max_swt ) :
				max_swt = result_list[len(result_list)-1][1][1]
			sum_swt += result_list[len(result_list)-1][1][1]
		output.write("\n")
	output.write("\n\n")

	# Blocking time stats write
	stats_output.write("\tBlocking Time: " + str( max_swt - float(sum_swt) / len(part_list)) + "ms\n")
	stats_output.write("\n")

def output_stage_shuffle_info(rdd_partition_shuffle_write, rdd_partition_shuffle_read, stage_id, output, verbose):
	part_list_write = rdd_partition_shuffle_write[stage_id]
	part_list_read = rdd_partition_shuffle_read[stage_id]

	output.write("\t\t\tShuffle Write Timing:[" + str(len(part_list_write)) + "]\n")
	if ( verbose == 1 ):
		for part_id, time_interval in part_list_write.iteritems():
				output.write("\t\t\tPartition_" + str(part_id) + " start from :" + str(milli2Readable(time_interval[0]))\
				+  " , end at: " + str(milli2Readable(time_interval[1])) + ", Lasting : " \
				+ str(long(time_interval[1]) - long(time_interval[0])) +  "ms  \n")
	else:
		[straggler_id, start_time, end_time] = getSlowestPartitionTime(part_list_write)
		output.write("\t\t Start:" + str(milli2Readable(start_time)) + " End: " + str(milli2Readable(end_time)) \
					+ "  Lasting: " + str(end_time - start_time ) + "ms" + "\n"
					)
	output.write("\n\n")

	output.write("\t\t\tShuffle Read Timing:["+ str(len(part_list_read)) +"]\n")
	if ( verbose == 1 ) :
		for part_id, time_interval in part_list_read.iteritems():
				output.write("\t\t\tPartition_" + str(part_id) + " start from :" + str(milli2Readable(time_interval[0]))\
				+  " , end at: " + str(milli2Readable(time_interval[1])) + ", Lasting : " \
				+ str(long(time_interval[1]) - long(time_interval[0])) +  "ms  \n")
	else:
		[straggler_id, start_time, end_time] = getSlowestPartitionTime(part_list_read)
		output.write("\t\t Start:" + str(milli2Readable(start_time)) + " End: " + str(milli2Readable(end_time)) \
					+ "  Lasting: " + str(end_time - start_time ) + "ms" + "\n"
					)
	output.write("\n\n")


# This calculate the actual blocking time of shuffle write.
def calculate_total_shuffle_write_time(rdd_partition_shuffle_write, output):
	total_time = 0
	for stage_id, part_list in rdd_partition_shuffle_write.iteritems():
		[straggler_id, start_time, end_time] = getSlowestPartitionTime(part_list)
		output.write("\t\t Start:" + str(milli2Readable(start_time)) + " End: " + str(milli2Readable(end_time)) \
		+ "  Lasting: " + str(end_time - start_time ) + "ms" + "\n"
		)
		total_time += (end_time - start_time)

	print total_time, "ms"

def main(argv):
	# This part is put to enable the verbose.
	inputfile = ''
	outputfile = ''
	try:
  		opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
	except getopt.GetoptError:
		print 'extra_logParser.py -i <inputfile> -o <outputfile>'
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print 'extra_logParser.py -i <inputfile> -o <outputfile>'
			sys.exit()
		elif opt in ("-i", "--ifile"):
			inputfile = arg
		elif opt in ("-o", "--ofile"):
			outputfile = arg
	print 'Input file is "', inputfile
	print 'Output file is "', outputfile

	# Switch to control if each partitions' time can be output.
	verbose = 0

	# Main program
	with open (output, 'w') as f :
		with open(filename, 'r') as fd :
			for line in fd:
				# ***  Parsing  ***
				# First 36 characters, just strip it.
				line = line[37:]
				split_result = line.split(",")
				rdd_name = ""
				rdd_id = ""
				shuffle_id_or_reader_tag = ""
				shuffle_id = ""
				reader_rdd = ""
				# task_type can be: ShuffleMapTask, ResultTask, iterator.FromParent, iterator.FromCache
				if (len(split_result) == 4 ):
					[task_type, task_attmpt_id, part_id, stage_id] = split_result
				elif (len(split_result) == 5 ):
					[task_type, shuffle_id_or_reader_tag, task_attmpt_id, part_id, stage_id] = split_result
				elif ( len(split_result) == 6):
					[task_type, rdd_id, rdd_name, task_attmpt_id, part_id, stage_id] = split_result

				# Hanlde common parameters
				stage_id = int(stage_id.split(":")[1])
				part_id = int(part_id[(part_id.find(":")+1):])
				task_attmpt_id = int(task_attmpt_id[(task_attmpt_id.find(":")+1):])
				task_name = task_type.split("]")[0]
				# End or start is not important, just make sure each operation's time interval has two elements.
				timestamp = long(task_type.split("]")[1].split(":")[1])

				# Handle special parameters
				if (len(split_result) == 6 ):
					rdd_name = rdd_name.split(":")[1]
					rdd_id = int(rdd_id.split(":")[1])
					rdd_id_name_map[rdd_id] = rdd_name
					# ***  Info Assemble  ***
					stage_info[stage_id][rdd_id][part_id].append(timestamp)
					stage_single_part_info[stage_id][part_id][rdd_id].append(timestamp)
				elif (len(split_result) == 5 ):
					# Writer info
					if ( ':' in shuffle_id_or_reader_tag):
						shuffle_id = int(shuffle_id_or_reader_tag.split(":")[1])
						rdd_partition_shuffle_write[stage_id][part_id].append(timestamp)
					# Reader info
					else:
						reader_rdd = shuffle_id_or_reader_tag
						rdd_partition_shuffle_read[stage_id][part_id].append(timestamp)
				elif (len(split_result) == 4):
					stage_id_name_map[stage_id] = task_name
					stage_time_map[stage_id][part_id].append(timestamp)

		# Output all the stage.
		fd2 = open("refined_output", "w")
		fd3 = open("stats", "w")
		for stage_id, rdd_list in stage_info.iteritems():
			output_stage_info( stage_info, stage_id, f, stage_id_name_map, rdd_id_name_map, verbose)
			output_part_oriented_stage_info(stage_single_part_info, stage_id, fd2, stage_id_name_map, rdd_id_name_map,\
										rdd_partition_shuffle_write, verbose, fd3)

			if (stage_id_name_map[stage_id][0] == 'S'):
				output_stage_shuffle_info(rdd_partition_shuffle_write, rdd_partition_shuffle_read, stage_id, f, verbose)
		calculate_total_shuffle_write_time(rdd_partition_shuffle_write, f)
		fd.close()
	f.close()






if __name__ == "__main__":
   main(sys.argv[1:])
