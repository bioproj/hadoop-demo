import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
    public static void main(String[] args) throws Exception {

        Job job = new Job();
        job.setJobName("word count");
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path("/home/wangyang/workspace/hadoop/hadoop-demo/workDir/input.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("/home/wangyang/workspace/hadoop/hadoop-demo/workDir/output"));
        FileInputFormat.addInputPath(job, new Path("hdfs://server:8020/user/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://server:8020/user/output3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
