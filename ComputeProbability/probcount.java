import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.HashMap;
import java.io.IOException;

public class probcount {
	
		public static class Map
	  extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String line=value.toString();
			String[] stats=line.split(",");
			Text outputKey = new Text();
			Text outputValue = new Text();
    			
			outputKey.set(stats[10]+","+stats[11]);
			outputValue.set(stats[2]+","+
					stats[3]+","+
					stats[4]+","+
					stats[5]+","+
					stats[6]+","+
					stats[7]+","+
					stats[8]+","+
					stats[9]
					);
			context.write(outputKey,outputValue);
		}
	}
	
	
	
	/*public static class Map2
	  extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int m = Integer.parseInt(conf.get("m"));
			int p = Integer.parseInt(conf.get("p"));
			String line = value.toString();
			// (M, i, j, Mij);
			String[] indicesAndValue = line.split(",");
			Text outputKey = new Text();
			Text outputValue = new Text();
			if (indicesAndValue[0].equals("M")) {
				for (int k = 0; k < p; k++) {
					outputKey.set(indicesAndValue[1] + "," + k);
					// outputKey.set(i,k);
					outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2]
							+ "," + indicesAndValue[3]);
					// outputValue.set(M,j,Mij);
					context.write(outputKey, outputValue);
				}
			} else {
				// (N, j, k, Njk);
				for (int i = 0; i < m; i++) {
					outputKey.set(i + "," + indicesAndValue[2]);
					outputValue.set("N," + indicesAndValue[1] + ","
							+ indicesAndValue[3]);
					context.write(outputKey, outputValue);
				}
			}
		}
	}*/
	
	
	
	
	
	
		public static class Reduce
	  extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String[] value;
			/*//key=(i,k),
			//Values = [(M/N,j,V/W),..]
			*/
			int sum0=0,sum1=0,sum2=0,sum3=0,sum4=0,sum5=0,sum6=0,sum7=0;
			for (Text val : values) {
				value = val.toString().split(",");
				sum0+=Integer.parseInt(value[0]);
				sum1+=Integer.parseInt(value[1]);
				sum2+=Integer.parseInt(value[2]);
				sum3+=Integer.parseInt(value[3]);
				sum4+=Integer.parseInt(value[4]);
				sum5+=Integer.parseInt(value[5]);
				sum6+=Integer.parseInt(value[6]);
				sum7+=Integer.parseInt(value[7]);
			}
			context.write(null,new Text(key.toString() + "," + Integer.toString(sum0)+","+Integer.toString(sum1)+","+Integer.toString(sum2)+","+Integer.toString(sum3)+","+Integer.toString(sum4)+","+Integer.toString(sum5)+","+Integer.toString(sum6)+","+Integer.toString(sum7)));
		}
	}
	
	
	
	
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
            System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
            System.exit(2);
        }
    	Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "probcount");
        job.setJarByClass(probcount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);		
 	MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Map.class);
 		//MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Map2.class);
 		
        //job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        //job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }
}
