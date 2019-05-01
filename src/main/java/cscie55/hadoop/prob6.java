package cscie55.hadoop;

import java.io.IOException;
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

public class prob6 {


    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }

        public int[] toIntArray() {
            Writable[] w = this.get();
            int[] a = new int[w.length];
            for (int i = 0; i < a.length; ++i) {
                a[i] = Integer.parseInt(w[i].toString());
            }
            return a;
        }
    }


    public static class MyMapper extends
            Mapper<Object, Text, Text, IntArrayWritable>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            // Convert the Text object for the value to a String.
            String line = value.toString();

            // Split the line on the spaces to get an array containing
            // the individual words.
            String[] words = line.split(",");
            String[] words2 = line.split(";");

            // Process the words one at a time, writing a key-value pair
            // for each of them.
            for (String word : words2) {
                if(word.contains("@") || word.contains("-")) {
                    continue;
                }
                String parts[] = word.split(",");

                IntWritable parts2[] = new IntWritable[parts.length];
                Integer counter = 0;
                for (String part : parts) {
                    parts2[counter] = new IntWritable(Integer.parseInt(part));
                    counter++;

                    String pass_text = "(" + words[0] + "," + part + ")";
                    IntArrayWritable pass_int = new IntArrayWritable(parts2);

                    context.write(new Text(pass_text), pass_int);

                }


            }
        }
    }


    public static class MyReducer extends
            Reducer<Text, IntArrayWritable, Text, IntArrayWritable>
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


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        // Specifies the name of the outer class.
        job.setJarByClass(prob6.class);

        // Specifies the names of the mapper and reducer classes.
        job.setMapperClass(prob6.MyMapper.class);
        job.setReducerClass(prob6.MyReducer.class);

        // Sets the type for the keys output by the mapper and reducer.
        job.setOutputKeyClass(Text.class);

        // Sets the type for the values output by the mapper and reducer,
        // although we can--and do in this case--change the mapper's type below.
        job.setOutputValueClass(IntArrayWritable.class);

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
        job.setMapOutputValueClass(IntArrayWritable.class);


        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
