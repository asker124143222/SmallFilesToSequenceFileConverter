import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/2/18 12:26
 * @Description:
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable,BytesWritable> {
    /**
     * Is the given filename splittable? Usually, true, but if the file is
     * stream compressed, it will not be.
     * <p>
     * The default implementation in <code>FileInputFormat</code> always returns
     * true. Implementations that may deal with non-splittable files <i>must</i>
     * override this method.
     * <p>
     * <code>FileInputFormat</code> implementations can override this and return
     * <code>false</code> to ensure that individual input files are never split-up
     * so that  process entire files.
     *
     * @param context  the job context
     * @param filename the file name to check
     * @return is this file splitable?
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * Create a record reader for a given split. The framework will call
     * {@link RecordReader#initialize(InputSplit, TaskAttemptContext)} before
     * the split is used.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeFileRecordReader recordReader = new WholeFileRecordReader();
        recordReader.initialize(split,context);
        return recordReader;
    }
}
