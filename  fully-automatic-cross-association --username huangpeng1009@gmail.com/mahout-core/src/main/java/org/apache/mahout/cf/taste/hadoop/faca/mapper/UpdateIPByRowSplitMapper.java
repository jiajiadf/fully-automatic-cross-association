package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAForItemWithGNWritable;

public class UpdateIPByRowSplitMapper extends Mapper<LongWritable, PAForItemWithGNWritable, LongWritable, PAForItemWithGNWritable>{
	
}
