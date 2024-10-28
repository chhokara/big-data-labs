import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {
    public static class WikiMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private IntWritable visitCount = new IntWritable();
        private Text dateTime = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\\s+");

            String dtString = items[0];
            String locale = items[1];
            String wikiName = items[2];
            Integer vCount = Integer.parseInt(items[3]);

            if (locale.equals("en") && !wikiName.equals("Main_Page") && !wikiName.startsWith("Special:")) {
                visitCount.set(vCount);
                dateTime.set(dtString);
                context.write(dateTime, visitCount);
            }
        }
    }

    public static class WikiReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int mostViewed = 0;
            for (IntWritable val : values) {
                mostViewed = Math.max(mostViewed, val.get());
            }
            result.set(mostViewed);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wiki job");
        job.setJarByClass(WikipediaPopular.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(WikiMapper.class);
        job.setCombinerClass(WikiReducer.class);
        job.setReducerClass(WikiReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}