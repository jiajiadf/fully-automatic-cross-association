package org.apache.mahout.cf.taste.hadoop.faca;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ReaderTest {
	public static void main(String[] args) throws IOException {
		String uri = "./temp/userPrefsWritable/part-r-00000";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		SequenceFile.Reader reader = null;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while(reader.next(key, value)){
				System.out.print(key + " / ");
				Vector vector = ((VectorWritable)value).get();
				for(Iterator<Vector.Element> it = vector.iterateNonZero(); it.hasNext(); ){
					Vector.Element element = it.next();
					System.out.print(element.index() + ":" + element.get() + " ");
				}
				System.out.println();
			}
		}
		finally {
			IOUtils.closeStream(reader);
		}
	}
}
