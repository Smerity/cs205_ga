package org.cs205.ccga;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.hadoop.mapred.ArcInputFormat;
import org.commoncrawl.hadoop.mapred.ArcRecord;

public class GACount extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(GACount.class);
  // Command line options
  private static final String ARGNAME_INPATH = "-in";
  private static final String ARGNAME_OUTPATH = "-out";
  private static final String ARGNAME_CONF = "-conf";
  private static final String ARGNAME_OVERWRITE = "-overwrite";
  private static final String ARGNAME_MAXFILES = "-maxfiles";
  private static final String ARGNAME_NUMREDUCE = "-numreducers";
  private static final String ARGNAME_TOTALSEGMENTS = "-totalsegments";
  private static final String FILEFILTER = ".arc.gz";

  protected static enum MAPPERCOUNTER {
    NOT_RECOGNIZED_AS_HTML, HTML_PAGE_TOO_LARGE, EXCEPTIONS, OUT_OF_MEMORY, HTML_IS_EMPTY
  }

  protected static enum GACOUNTER {
    GA, NO_GA
  }

  public static class GAMapper extends
      Mapper<Text, ArcRecord, Text, LongWritable> {

    // We create these outside of the map so they can be re-used
    // This prevents having to re-allocate + create the objects each run
    private String url;
    private LongWritable outVal = new LongWritable(1);

    public void map(Text key, ArcRecord value, Context context) throws IOException {

      try {
        // Convert the URL immediately to the host domain format
        // i.e. google.com/xyz/abc => google.com
        url = new URL(value.getURL()).getHost();

        // If the content isn't HTML, skip
        if (!value.getContentType().contains("html")) {
          context.getCounter(MAPPERCOUNTER.NOT_RECOGNIZED_AS_HTML).increment(1);
          return;
        }

        // If the HTML page is excessively large, skip
        if (value.getContentLength() > (5 * 1024 * 1024)) {
          context.getCounter(MAPPERCOUNTER.HTML_PAGE_TOO_LARGE).increment(1);
          return;
        }

        String contents = value.getHTML();
        // If the HTML is empty, skip
        if (contents == null) {
          context.getCounter(MAPPERCOUNTER.HTML_IS_EMPTY).increment(1);
          return;
        }

        // Check whether the Google Analytics Javascript code is on their page
        if (contents.contains("google-analytics.com/ga.js")) {
          context.write(new Text(url + " GA"), outVal);
          context.getCounter(GACOUNTER.GA).increment(1);
        } else {
          context.write(new Text(url + " NoGA"), outVal);
          context.getCounter(GACOUNTER.NO_GA).increment(1);
        }
      } catch (Throwable e) {
        // Occasionally we get an exception
        // If it's OOM, we can recover
        // We call the garbage collector to try to free as much memory as possible
        if (e.getClass().equals(OutOfMemoryError.class)) {
          context.getCounter(MAPPERCOUNTER.OUT_OF_MEMORY).increment(1);
          System.gc();
        }

        // If the exception is a different one, we record it silently and continue
        LOG.error("Caught Exception", e);
        context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
      }
    }
  }

  /**
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param args
   *            command line parameters, less common Hadoop job parameters
   *            stripped out and interpreted by the Tool class.
   * @return 0 if the Hadoop job completes successfully, 1 if not.
   */
  @Override
  public int run(String[] args) throws Exception {

    String inputPath = null;
    String outputPath = null;
    String configFile = null;
    boolean overwrite = false;
    int numReducers = 1;
    int totalSegments = 1;

    // Read the command line arguments. We're not using GenericOptionsParser
    // to prevent having to include commons.cli as a dependency.
    for (int i = 0; i < args.length; i++) {
      try {
        if (args[i].equals(ARGNAME_INPATH)) {
          inputPath = args[++i];
        } else if (args[i].equals(ARGNAME_OUTPATH)) {
          outputPath = args[++i];
        } else if (args[i].equals(ARGNAME_CONF)) {
          configFile = args[++i];
        } else if (args[i].equals(ARGNAME_MAXFILES)) {
          MaxFileFilter.setMax(Long.parseLong(args[++i]));
        } else if (args[i].equals(ARGNAME_OVERWRITE)) {
          overwrite = true;
        } else if (args[i].equals(ARGNAME_NUMREDUCE)) {
          numReducers = Integer.parseInt(args[++i]);
        } else if (args[i].equals(ARGNAME_TOTALSEGMENTS)) {
                totalSegments = Integer.parseInt(args[++i]);
          } else {
          LOG.warn("Unsupported argument: " + args[i]);
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        LOG.error("Command line usage was incorrect -- see documentation");
          GenericOptionsParser.printGenericCommandUsage(System.out);
        throw new IllegalArgumentException();
      }
    }

    // Read in any additional config parameters.
    if (configFile != null) {
      LOG.info("adding config parameters from '" + configFile + "'");
      this.getConf().addResource(configFile);
    }

    // Create the Hadoop job.
    Configuration conf = getConf();
    Job job = new Job(conf);
    job.setJarByClass(GACount.class);
    job.setNumReduceTasks(numReducers);
    // If so desired, you can hard code the number of mappers
    //conf.setInt("mapred.map.tasks", numMappers);

    // If an input path is specified, run locally
      if (inputPath != null) {
        LOG.info("Running locally...");
        LOG.info("Adding input path '" + inputPath + "'");
        FileInputFormat.addInputPath(job, new Path(inputPath));
      } else {
        LOG.info("Running job using files from Amazon S3...");
        LOG.info("[WARNING: This might cost money!]");
        LOG.info("Adding a total of " + totalSegments + " segments");
        // The segment list provides all valid segments from the 2012 crawl
        String segmentListFile = "s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt";
        // Add as many segments as we desire to the input path
        FileSystem fs = FileSystem.get(new URI(segmentListFile), conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(segmentListFile))));
        String segmentId; int segmentsAdded = 0;
        while ((segmentId = reader.readLine()) != null &&  segmentsAdded++ < totalSegments) {
          LOG.info("Adding segment '" + segmentId + "'");
          String segmentPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/"+segmentId+"/*.arc.gz";
          FileInputFormat.addInputPath(job, new Path(segmentPath));
        }
      }
      MaxFileFilter.setFilter(FILEFILTER);
      FileInputFormat.setInputPathFilter(job, MaxFileFilter.class);

    // Delete the output path directory if it already exists and user wants to overwrite it.
    if (overwrite) {
      LOG.info("Clearing the output path at '" + outputPath + "'");
      FileSystem fs = FileSystem.get(new URI(outputPath), conf);
      if (fs.exists(new Path(outputPath))) {
        fs.delete(new Path(outputPath), true);
      }
    }

    // Set the path where final output 'part' files will be saved.
    LOG.info("setting output path to '" + outputPath + "'");
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    // Set which InputFormat class to use.
    job.setInputFormatClass(ArcInputFormat.class);

    // Set which OutputFormat class to use.
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // Set which Mapper, Combiner, and Reducer classes to use.
    job.setMapperClass(GACount.GAMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return 1;
    }
  }

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the
   * example Hadoop job.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // Should the whole MR job use compression?
    conf.setBoolean("mapred.output.compress", true);
    // Use Gzip for final compressed output
    conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
    // How to compress data between the map and reduce portions
    conf.setBoolean("mapred.compress.map.output", true);
    // Use Snappy for faster compression between Map and Reduce
    conf.setClass("mapred.map.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
    // Allow for a portion of the input to fail without killing the job
    // NOTE: We don't expect this much to occur, but the user would intervene
    conf.setInt("mapred.max.map.failures.percent", 10);
    // Add your S3 AWS access credentials
    conf.setStrings("fs.s3n.awsAccessKeyId", "<YOUR AWS ACCESS ID>");
    conf.setStrings("fs.s3n.awsSecretAccessKey", "<YOUR AWS SECRET KEY>");
    //
    int res = ToolRunner.run(conf, new GACount(), args);
    System.exit(res);
  }
}
