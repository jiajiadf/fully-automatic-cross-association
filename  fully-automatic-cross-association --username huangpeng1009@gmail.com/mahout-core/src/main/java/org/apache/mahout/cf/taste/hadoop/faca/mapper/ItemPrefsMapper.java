package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemPrefsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
		if(line.toString().length() != 0){
			String[] tokens = line.toString().split("[,\t]");
			//Input line MUST have at least 3 fields
			long itemID = Long.parseLong(tokens[1]);
			String userID = tokens[0];
			String pref = tokens[2];
			context.write(new LongWritable(itemID), new Text(userID + ":" + pref));		
		}
	}
}