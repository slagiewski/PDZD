import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhotosAdvanced {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        static String SEPARATOR = "\u001f";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line == null || line.isEmpty()) {
                return;
            }

            String businessId = null;
            String label = null;

            String[] els = line.split(SEPARATOR);
            if (els.length == 4) {
                businessId = els[0];
                label = els[2];
            } else {
                System.out.println("Invalid line: " + line);
                return;
            }

            int intWr = -1;
            if (label.equals("food")) {
                intWr = 0;
            } else if (label.equals("drink")) {
                intWr = 1;
            } else if (label.equals("inside")) {
                intWr = 2;
            } else if (label.equals("outside")) {
                intWr = 3;
            }

            context.write(new Text(businessId), new Text(Integer.toString(intWr)));
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            int foodCategoryCounter = 0;
            int drinkCategoryCounter = 0;
            int insideCategoryCounter = 0;
            int outsideCategoryCounter = 0;

            if (key.toString().length() != 22) {
                return;
            }

            for (Text value : values) {
                counter++;
                int x = 0;
                try {
                    x = Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    System.out.println("Invalid value: " + value.toString());
                    return;
                }
                switch (x) {
                case 0:
                    foodCategoryCounter++;
                    break;
                case 1:
                    drinkCategoryCounter++;
                    break;
                case 2:
                    insideCategoryCounter++;
                    break;
                case 3:
                    outsideCategoryCounter++;
                    break;
                }
            }

            String result = foodCategoryCounter + "," + drinkCategoryCounter + "," + insideCategoryCounter + ","
                    + outsideCategoryCounter + "," + counter;

            context.write(new Text(key), new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "photos count advanced");
        job.setJarByClass(PhotosAdvanced.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
