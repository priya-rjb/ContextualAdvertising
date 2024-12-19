/**
 * MapReduce job that pipes input to output as MapReduce-created key-val pairs
 */

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
/**
 * Trivial MapReduce job that pipes input to output as MapReduce-created key-value pairs.
 */
public class Advertising {
    /** Creating the Parser Instance: */
    private static JSONParser parser = new JSONParser();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Error: Wrong number of parameters");
            System.err.println("Expected: [impressions] [clicks] [out]");
            System.exit(1);
        }
        // pecify the paths to the files here:
        Path impressionsInput = new Path(args[0]);
        Path clicksInput = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        Path Intermediate_Directory = new Path("Temp");

        // Only need one configuration
        Configuration conf = new Configuration();

        // Specify two jobs here
        Job job1 = Job.getInstance(conf, "first job");
        job1.setJarByClass(Advertising.class);

        // set the Mapper and Reducer functions we want
        job1.setMapperClass(Advertising.ImpressionsMapper.class);
        job1.setReducerClass(Advertising.FirstReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Job 1 configuration
        job1.setMapOutputKeyClass(Text.class);  // Set key type for mapper output
        job1.setMapOutputValueClass(Text.class);  // Set value type for mapper output
        job1.setOutputKeyClass(Text.class);  // Set key type for reducer output
        job1.setOutputValueClass(Text.class);  // Set value type for reducer output


        //Create a neew temporary path
        Path tempPath = Intermediate_Directory;
        // input arguments tell us where to get/put things in HDFS
        // Add the input paths here: 
        FileInputFormat.addInputPath(job1, impressionsInput);
        FileInputFormat.addInputPath(job1, clicksInput);
        FileOutputFormat.setOutputPath(job1, tempPath);

        // create a temporary directory and set the output path of the job 
        // ternary operator - a compact conditional that just returns 0 or 1
        if (job1.waitForCompletion(true)){
            Job job2 = Job.getInstance(conf, "second job");
            job2.setJarByClass(Advertising.class);

            job2.setReducerClass(Advertising.SecondReducer.class);
            
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            // Job 2 configuration
            job2.setMapOutputKeyClass(Text.class);  // Set key type for mapper output
            job2.setMapOutputValueClass(Text.class);  // Set value type for mapper output
            job2.setOutputKeyClass(Text.class);  // Set key type for reducer output
            job2.setOutputValueClass(Text.class);  // Set value type for reducer output

            FileInputFormat.addInputPath(job2, tempPath); // in the same file
            FileOutputFormat.setOutputPath(job2, outputPath);

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else{
            System.exit(1);
        }
    }

    /**
     * map: (LongWritable, Text) --> (Text, Text)
     * NOTE: Keys must implement WritableComparable, values must implement Writable
     */
    public static class ImpressionsMapper extends Mapper < LongWritable, Text, 
                                                        Text, Text > {

        @Override
        public void map(LongWritable key, Text val, Context context)
            throws IOException, InterruptedException {
                try {
                // Parses JSON object from each line of input
                JSONObject jsonObject = (JSONObject) parser.parse(val.toString());

                // Extract the impressionId, which will be used as the key
                String impressionId = (String) jsonObject.get("impressionId");

                // Write (key, val) out to memory/disk
                // Emit the impressionId as the key and the JSON as the value
                // context.write(new Text(impressionId), val);
                context.write(new Text(impressionId), val);

                } catch (ParseException e) {
                    // Handle parse errors
                    System.err.println("Parse error for line: " + val.toString());
                    e.printStackTrace();
                }
        }
    }

    /**
     * reduce: (Text, Text) --> (Text, Text)
     */
    public static class FirstReducer extends Reducer < Text, Text, 
                                                        Text, Text > {

        @Override
        public void reduce(Text key, Iterable < Text > values, Context context) 
            throws IOException, InterruptedException {
            String adId = null;
            String referrer = null;
            String isClicked = null;

            JSONObject clickJSON = null;
            JSONObject impressionJSON = null;

            // Store click and impression entries
            for (Text val : values) {
                try {
                    JSONObject jsonObject = (JSONObject) parser.parse(val.toString());

                    // Check if it's a click based on a specific field, e.g., "isClick" or similar
                    if (jsonObject.containsKey("referrer")) {
                        impressionJSON = jsonObject;
                    } else {
                        clickJSON = jsonObject;
                    }
                } catch (ParseException e) {
                    System.err.println("Parse error for value: " + val.toString());
                    e.printStackTrace();
                }
            }

            try {
                adId = (String) impressionJSON.get("adId");
                referrer = (String) impressionJSON.get("referrer");
                
                // Case 1: Both click and impression are present
                if (clickJSON != null) {
                    isClicked = "1";

                // Case 2: Only impression is present
                } else { 
                    isClicked = "0";
                }

                // Write (key, val) out to memory/disk
                // Emit the adId, referrer as the key and either "0" or "1" (indicating a click) as the value
                context.write(new Text("[" + referrer + ", " + adId + "]"), new Text(isClicked));

            } catch (Exception e) {
                System.err.println("Error when processing JSON objects.");
                e.printStackTrace();
            }
        }
    }
    
    /**
     * reduce: (Text, Text) --> (Text, Text)
     */
    public static class SecondReducer extends Reducer < Text, Text, 
                                                        Text, Text > {
        @Override
        public void reduce(Text key, Iterable < Text > values, Context context) 
            throws IOException, InterruptedException {

                double clicks = 0.0;
                double impressions = 0.0;
                double clickRate = 0.0;

                // Iterate through each val (0: impression, 1: click and impression)
                // in the Iterable grouped with the key (referrer, ad_id):
                for (Text val : values) {
                    // Click and impression
                    if (val.toString().equals("1")) {
                        clicks++;
                    }
                    // Just impression
                    impressions++;
                }

                // Avoid Floating Point Error i.e division by zero
                // Calculate CTR as long as there is at least one impression
                if (impressions > 0) {
                    clickRate = clicks / impressions;
                }
                // write (key, val) for every value: key is still [referrer, ad_id] and value: clickRate
                // The Text class in Hadoop’s API expects a String as input
                context.write(key, new Text(String.valueOf(clickRate)));
        }
    }
}