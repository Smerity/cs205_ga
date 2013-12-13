package org.commoncrawl.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A input format the reads arc files.
 */
public class ArcInputFormat extends FileInputFormat<Text, ArcRecord> {

  /**
   * Returns the <code>RecordReader</code> for reading the arc file.
   * 
   * @param split
   *          The InputSplit of the arc file to process.
   * @param job
   *          The job configuration.
   * @param reporter
   *          The progress reporter.
   */
  public RecordReader<Text, ArcRecord> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    context.setStatus(split.toString());
    return new ArcRecordReader();
  }

  /**
   * <p>
   * Always returns false to indicate that ARC files are not splittable.
   * </p>
   * <p>
   * ARC files are stored in 100MB files, meaning they will be stored in at most
   * 3 blocks (2 blocks on Hadoop systems with 128MB block size).
   * </p>
   */
  protected boolean isSplitable(JobContext context, org.apache.hadoop.fs.Path filename) {
    return false;
  }
}
