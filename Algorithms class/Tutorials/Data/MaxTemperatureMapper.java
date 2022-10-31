import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
iimport org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	  private Text yearText = new Text();
	  private final static IntWritable tempWritable = new IntWritable(0);
	
	  protected void map(LongWritable key, Text value, Context context)
	      throws java.io.IOException, InterruptedException {
	    String[] line = value.toString().split(";");
	    String year = line[0];
	    yearText.set(year);
	    int temp = Integer.parseInt(line[1]);
	    tempWritable.set(temp);
	    context.write(yearText,tempWritable);
	  }  
}