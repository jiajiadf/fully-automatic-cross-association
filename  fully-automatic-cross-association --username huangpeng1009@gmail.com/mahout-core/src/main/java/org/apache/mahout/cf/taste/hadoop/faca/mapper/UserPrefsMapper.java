package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserPrefsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
		if (line.toString().length() != 0) {
			String[] tokens = line.toString().split("[,\t]");
			// Input line MUST have at least 3 fields
			long userID = Long.parseLong(tokens[0]);
			String itemID = tokens[1];
			String pref = tokens[2];
			context.write(new LongWritable(userID), new Text(itemID + ":" + pref));
		}
	}
}
