package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class CopyMapper extends Mapper<Writable, Writable, Writable, Writable>{

	@Override
	protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
		super.map(key, value, context);
	}
	
}
