import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmit {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        //1. 设置job运行时需要访问的默认文件系统
        conf.set("fs.defaultFS", "hdfs://server:8020");
        //2. 设置job提交到哪里去
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "server");

        Job job = new Job(conf);
        job.setJobName("word count1");
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        FileInputFormat.addInputPath(job, new Path("/home/wangyang/workspace/hadoop/hadoop-demo/workDir/input.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("/home/wangyang/workspace/hadoop/hadoop-demo/workDir/output1"));
        FileInputFormat.addInputPath(job, new Path("hdfs://server:8020/user/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://server:8020/user/output10"));
//        FileInputFormat.addInputPath(job, new Path("/user/input.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("/user/output6"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
