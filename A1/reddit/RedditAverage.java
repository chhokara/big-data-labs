import java.io.IOException;

import org.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RedditAverage extends Configured implements Tool {
    public static class RedditJsonMapper
            extends Mapper<LongWritable, Text, Text, LongPairWritable> {
        // Mapper code
        private final static long one = 1L;
        private final static LongPairWritable countScorePair = new LongPairWritable();
        private Text subreddit = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject record = new JSONObject(value.toString());

            String subredditName = record.getString("subreddit");
            long score = record.getLong("score");

            subreddit.set(subredditName);
            countScorePair.set(one, score);

            context.write(subreddit, countScorePair);

        }

    }

    public static class RedditAverageCombiner
            extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
        // Combiner code
        private final static LongPairWritable intermediateCountScorePair = new LongPairWritable();

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values, Context context)
                throws IOException, InterruptedException {
            long countSum = 0;
            long scoreSum = 0;

            for (LongPairWritable val : values) {
                countSum += val.get_0();
                scoreSum += val.get_1();
            }

            intermediateCountScorePair.set(countSum, scoreSum);
            context.write(key, intermediateCountScorePair);
        }

    }

    public static class RedditAverageReducer
            extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
        // Reducer code
        private final static DoubleWritable average = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values, Context context)
                throws IOException, InterruptedException {
            long countSum = 0;
            long scoreSum = 0;

            for (LongPairWritable val : values) {
                countSum += val.get_0();
                scoreSum += val.get_1();
            }

            Double avg = (double) (scoreSum) / (double) (countSum);
            average.set(avg);
            context.write(key, average);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "reddit comments");
        job.setJarByClass(RedditAverage.class);

        job.setInputFormatClass(TextInputFormat.class);

        // Setting mapper, reducer, and combiner
        job.setMapperClass(RedditJsonMapper.class);
        job.setReducerClass(RedditAverageReducer.class);
        job.setCombinerClass(RedditAverageCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
