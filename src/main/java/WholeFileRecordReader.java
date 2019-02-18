import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/2/18 12:12
 * @Description:
 */
public class WholeFileRecordReader extends RecordReader<NullWritable,BytesWritable> {
    private FileSplit fileSplit;
    private Configuration conf;
    private BytesWritable value = new BytesWritable();
    private boolean processed = false;
    /**
     * Called once at initialization.
     *
     * @param split   the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    /**
     * Read the next key, value pair.
     *
     * @return true if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            byte[] contents = new byte[(int)fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in,contents,0,contents.length);//一次全部读取
                value.set(contents,0,contents.length);
            }finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }

        return false;
    }

    /**
     * Get the current key
     *
     * @return the current key or null if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * Get the current value.
     *
     * @return the object that was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f:0.0f;
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close() throws IOException {

    }
}
