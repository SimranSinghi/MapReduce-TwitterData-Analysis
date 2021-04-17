import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Simple {
    public static class MyMapper extends Mapper<Object,Text,IntWritable,DoubleWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = s.nextInt();
            double y = s.nextDouble();
            context.write(new IntWritable(x),new DoubleWritable(y));
            s.close();
        }
    }

    public static class MyReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            for (DoubleWritable v: values) {
                sum += v.get();
                count++;
            };
            context.write(key,new DoubleWritable(sum/count));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Simple.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }
}
