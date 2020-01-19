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

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }

        @Override
        public String toString() {
            String[] arr = super.toStrings();
            String resultStr = "";
            if (arr.length == 5) {
                resultStr = arr[0] + "," + arr[1] + "," + arr[2] + "," + arr[3] + "," + arr[4];
            } else if (arr.length == 4) {
                resultStr = arr[0] + "," + arr[1] + "," + arr[2] + "," + arr[3];
            } else {
                resultStr = "";
            }

            return resultStr;
        }

    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            IntWritable[] intArr = new IntWritable[4];
            System.out.println(line);
            if (line == null || line.isEmpty()) {
                return;
            }
            String businessId = null;
            String label = null;

            String[] els = line.split(",");
            if (els.length == 4) {
                businessId = els[0];
                label = els[2];
            } else if (els.length < 4) {
                return;
            } else {
                String[] els1 = line.split(",\"");
                businessId = els1[0];
                String[] els2 = line.split("\",");
                if (els2.length != 2 || els2[1].split(",").length != 2) {
                    return;
                } else {
                    label = els2[1].split(",")[0];
                }
            }
            IntWritable intWr = new IntWritable(0);
            switch (label) {
            case "food":
                intWr = new IntWritable(1);
                break;
            case "drink":
                intWr = new IntWritable(2);
                break;
            case "inside":
                intWr = new IntWritable(3);
                break;
            case "outside":
                intWr = new IntWritable(4);
                break;
            }

            word.set(businessId);

            context.write(word, intWr);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            IntArrayWritable arr = new IntArrayWritable();
            IntWritable[] intArr = new IntWritable[5];
            int[] categorySums = { 0, 0, 0, 0, 0 };

            for (IntWritable val : values) {
                categorySums[4] += 1;
                switch (val.get()) {
                case 0:
                    categorySums[4] += 1;
                    break;
                case 1:
                    categorySums[0] += 1;
                    break;
                case 2:
                    categorySums[1] += 1;
                    break;
                case 3:
                    categorySums[2] += 1;
                    break;
                case 4:
                    categorySums[3] += 1;
                    break;
                }
            }

            intArr[0] = new IntWritable(categorySums[0]);
            intArr[1] = new IntWritable(categorySums[1]);
            intArr[2] = new IntWritable(categorySums[2]);
            intArr[3] = new IntWritable(categorySums[3]);
            intArr[4] = new IntWritable(categorySums[4]);

            arr.set(intArr);

            context.write(key, arr);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "photos count advanced");
        job.setJarByClass(PhotosAdvanced.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
