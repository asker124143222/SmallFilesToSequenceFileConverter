import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/2/18 11:54
 * @Description:
 */
public class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable> {
    enum FileCounter {
        FILENUM
    }
    private Text filenameKey;
    /**
     * Called once at the beginning of the task.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        Path path = ((FileSplit)split).getPath();
        filenameKey = new Text(path.toString());
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(filenameKey,value);
        //自定义计数器
        context.getCounter(FileCounter.FILENUM).increment(1);
        //动态计数器
        context.getCounter("FileNameList",filenameKey.toString()).increment(1);
    }
}
