import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: xu.dm
 * @Date: 2019/2/18 11:51
 * @Description:
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     * @return exit code.
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if(conf==null){
            return -1;
        }

        Path outPath = new Path(args[1]);
        FileSystem fileSystem = outPath.getFileSystem(conf);
        //删除输出路径
        if(fileSystem.exists(outPath))
        {
            fileSystem.delete(outPath,true);
        }

        Job job = Job.getInstance(conf,"SmallFilesToSequenceFile");
        job.setJarByClass(SmallFilesToSequenceFileConverter.class);

        job.setMapperClass(SequenceFileMapper.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);



        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
//        if(args.length!=2)
//        {
//            System.err.println("使用格式：SmallFilesToSequenceFileConverter <input path> <output path>");
//            System.exit(-1);
//        }

        long startTime = System.currentTimeMillis();

        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(),args);
        System.exit(exitCode);

        long endTime = System.currentTimeMillis();
        long timeSpan = endTime - startTime;
        System.out.println("运行耗时："+timeSpan+"毫秒。");
    }
}
