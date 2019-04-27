package cscie55.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class prob4 {


    public static class MyMapper extends
            Mapper<Object, Text, Text, IntWritable>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            // Convert the Text object for the value to a String.
            String line = value.toString();

            // Split the line on the spaces to get an array containing
            // the individual words.
            String[] words = line.split(",");

            // Process the words one at a time, writing a key-value pair
            // for each of them.
            for (String word : words) {
                if(word.contains("@"))
                {
                    String parts[] = word.split("\\@");
                    String tempWord = parts[1];
                    tempWord = tempWord.split(";")[0];
                    context.write(new Text(tempWord), new IntWritable(1));
                }

            }
        }
    }


    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            // Total the list of values associated with the word.
            long count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            context.write(key, new LongWritable(count));
        }
    }








    public static class MyMapper2 extends
            Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
//            String line = value.toString();
            String tempWord2 = "c";
            context.write(new Text(tempWord2), value);

//            String line = value.toString();
//            context.write(new Text(line), new IntWritable(1));


        }
    }



    public static class MyReducer2 extends
            Reducer<Text, Text, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            // Total the list of values associated with the word.
            long theCount = 999999;
            String theWord = "aaa";
            for (Text val : values) {
                String tempString = val.toString();
                String[] words = tempString.split("\t");
                if(Long.valueOf(words[1]) > theCount) {
                    continue;
                }
                theWord = words[0];
                theCount = Long.valueOf(words[1]);
            }
            String temp = "aaa";

            context.write(new Text(theWord), new LongWritable(theCount));
        }
    }





    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count");
        // Specifies the name of the outer class.
        job.setJarByClass(prob4.class);
        // Specifies the names of the mapper and reducer classes.
        job.setMapperClass(prob4.MyMapper.class);
        job.setReducerClass(prob4.MyReducer.class);
        // Sets the type for the keys output by the mapper and reducer.
        job.setOutputKeyClass(Text.class);
        // Sets the type for the values output by the mapper and reducer,
        // although we can--and do in this case--change the mapper's type below.
        job.setOutputValueClass(LongWritable.class);
        // Sets the type for the keys output by the mapper.
        // Not needed here because both the mapper and reducer's output keys
        // have the same type, but you can uncomment it as needed
        // and pass in the appropriate type.
        //   job.setMapOutputKeyClass(Text.class);
        // Sets the type for the values output by the mapper.
        // This is needed because it is different than the type specified
        // by job.setOutputValueClass() above.
        // If the mapper and reducer output values of the same type,
        // you can comment out or remove this line.
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);



        Job job2 = Job.getInstance(conf, "word count");
        // Specifies the name of the outer class.
        job2.setJarByClass(prob4.class);
        // Specifies the names of the mapper and reducer classes.
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        // Sets the type for the keys output by the mapper and reducer.
        job2.setOutputKeyClass(Text.class);
        // Sets the type for the values output by the mapper and reducer,
        // although we can--and do in this case--change the mapper's type below.
        job2.setOutputValueClass(LongWritable.class);
        // Sets the type for the keys output by the mapper.
        // Not needed here because both the mapper and reducer's output keys
        // have the same type, but you can uncomment it as needed
        // and pass in the appropriate type.
        //   job.setMapOutputKeyClass(Text.class);
        // Sets the type for the values output by the mapper.
        // This is needed because it is different than the type specified
        // by job.setOutputValueClass() above.
        // If the mapper and reducer output values of the same type,
        // you can comment out or remove this line.
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);










    }

}
