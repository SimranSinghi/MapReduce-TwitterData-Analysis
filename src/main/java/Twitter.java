import java.io.*;
import java.util.Scanner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import  org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Twitter {
    public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = s.nextInt();
            int y = s.nextInt();
            context.write(new IntWritable(y),new IntWritable(x));
            s.close();
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v: values) {
                count++;
            };
            context.write(key,new IntWritable(count));
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable,Text,IntWritable,IntWritable> {
        @Override
        public void map ( LongWritable key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int x1 = s.nextInt();
            int y1 = s.nextInt();
            context.write(new IntWritable(y1),new IntWritable(1));
            s.close();
        }
    }

    public static class MyReducer2 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v: values) {
                sum+=v.get();
            };
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Twitter.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Twitter.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        job2.waitForCompletion(true);
    }
}
