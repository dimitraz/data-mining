import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * Takes in BSCY4_MR.csv and aggregates given keys with values for
 * different fields.
 *
 * Changes made to the csv file were:
 * - Replaced commas with decimal points
 * - Replaced empty field values with 0.0
 * These changes were required to run the program. No other changes
 * were made (e.g. to apply consistent formatting in dates, as this was not
 * necessary)
 *
 * @author Dimitra Zuccarelli (20072495)
 */
public class Avocados {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text csv_key = new Text();
        private Text val = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Primary key: <Date>=<Region>=<Year>=<Type>
            String[] values = value.toString().split("=");
            String[] keys = Arrays.copyOfRange(values, 0, 4);

            // Append the key tokens together with a comma, which gives the final key to be reduced
            StringBuilder sb = new StringBuilder();
            for (String s : keys) {
                sb.append(s);
                sb.append(',');
            }
            csv_key.set(sb.toString());

            // Write value
            // e.g. AveragePrice=1.8
            String value_string = values[5] + "=" + values[4];
            val.set(value_string);

            context.write(csv_key, val);
        }
    }

    // This method references this stack overflow question:
    // https://stackoverflow.com/questions/38095902/group-correspoding-key-and-values
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] headings = new String[9];

            for (Text value : values) {
                String[] fields_values = value.toString().split("=");

                // Fields are:
                // AveragePrice, TotalVolume, 4046, 4225, 4770, TotalBags, SmallBags, LargeBags, XLargeBags
                // A switch statement is used to keep all of these in order
                switch (fields_values[0]) {
                    case "AveragePrice":
                        headings[0] = fields_values[1];
                        break;
                    case "TotalVolume":
                        headings[1] = fields_values[1];
                        break;
                    case "4046":
                        headings[2] = fields_values[1];
                        break;
                    case "4225":
                        headings[3] = fields_values[1];
                        break;
                    case "4770":
                        headings[4] = fields_values[1];
                        break;
                    case "TotalBags":
                        headings[5] = fields_values[1];
                        break;
                    case "SmallBags":
                        headings[6] = fields_values[1];
                        break;
                    case "LargeBags":
                        headings[7] = fields_values[1];
                        break;
                    case "XLargeBags":
                        headings[8] = fields_values[1];
                        break;
                }
            }

            StringBuilder sb = new StringBuilder();
            for (String s : headings) {
                if (sb.length() != 0) {
                    sb.append(',');
                }
                sb.append(s);
            }

            result.set(sb.toString());
            context.write(key, result);
        }
    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce Avocados!");
        job.setJarByClass(Avocados.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}