package org.apache.mahout.cf.taste.hadoop.faca.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.faca.Config;

public class GetItemGroupIDReducer extends Reducer<IntWritable, Text, NullWritable, NullWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> entropys, Context context) throws IOException, InterruptedException {
		double max = 0.0;
		int groupID = 0;
		for (Text text : entropys) {
			String line = text.toString();
			String[] tokens = line.split("_");
			double value = Double.parseDouble(tokens[1]);
			if (max < value) {
				max = value;
				groupID = Integer.parseInt(tokens[0]);
			}
		}
		context.getCounter(Config.INFO, Config.MAX_ENTROPY_COL_ID).increment(groupID);
	}
}
