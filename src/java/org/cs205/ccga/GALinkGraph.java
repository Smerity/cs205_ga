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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class GALinkGraph extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(GALinkGraph.class);
  // Command line options
  private static final String ARGNAME_INPATH = "-in";
  private static final String ARGNAME_OUTPATH = "-out";
  private static final String ARGNAME_CONF = "-conf";
  private static final String ARGNAME_OVERWRITE = "-overwrite";
  private static final String ARGNAME_COMBINER = "-combiner";
  private static final String ARGNAME_COMPRESSION = "-compression";
  private static final String ARGNAME_MAXFILES = "-maxfiles";
  private static final String ARGNAME_NUMREDUCE = "-numreducers";
  private static final String ARGNAME_TOTALSEGMENTS = "-totalsegments";
  private static final String FILEFILTER = "metadata-";

  protected static enum MAPPERCOUNTER {
    INVALID_URLS,
    EXCEPTIONS
  }

  public static class ExportLinkGraph
      extends Mapper<Text, Text, Text, LongWritable> {

  // We create these outside of the map so they can be re-used
  // This prevents having to re-allocate + create the objects each run
    private Text outKey = new Text();
    private String fromUrl;
    private String toUrl;
    private String json;

    private static final JsonParser jsonParser = new JsonParser();
    private JsonObject jsonObj;

    @Override
    public void map(Text key, Text value, Context context) throws IOException {

      json = value.toString();

      try {

        // See if the page has a successful HTTP code
        jsonObj = jsonParser.parse(json).getAsJsonObject();

        // If the request was not successful, move to the next record
        if (!jsonObj.has("disposition") || !jsonObj.get("disposition").getAsString().equals("SUCCESS")) {
          return;
        }
        // If the request is not HTML, move on to the next record
        if (!jsonObj.has("mime_type") || !jsonObj.get("mime_type").getAsString().equals("text/html")) {
          return;
        }

        // Count the number of links on the given page
        if (jsonObj.has("content") && jsonObj.getAsJsonObject("content").has("links")) {
            fromUrl = new URL(key.toString()).getHost();
          JsonArray links = jsonObj.getAsJsonObject("content").getAsJsonArray("links");
            for(JsonElement je : links) {
              try {
                if (je.getAsJsonObject().getAsJsonObject().get("type").getAsString().equals("a")) {
                  toUrl = new URL(je.getAsJsonObject().getAsJsonObject().get("href").getAsString()).getHost();
                  outKey.set(fromUrl + " -> " + toUrl);
                    context.write(outKey, new LongWritable(1));
                }
              } catch (java.net.MalformedURLException ex) {
                // Skip the link if it's not well formed
              }
            }
        }
      } catch (IOException ex) {
        throw ex;
      } catch (Exception ex) {
        LOG.error("Caught Exception", ex);
        context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
      }
    }
  }

  /**
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param  args command line parameters, less common Hadoop job parameters stripped
   *              out and interpreted by the Tool class.
   * @return      0 if the Hadoop job completes successfully, 1 if not.
   */
  @Override
  public int run(String[] args) throws Exception {

    String inputPath = null;
    String outputPath = null;
    String configFile = null;
    boolean overwrite = false;
    boolean combiner = false;
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
        } else if (args[i].equals(ARGNAME_COMBINER)) {
        combiner = true;
        } else if (args[i].equals(ARGNAME_TOTALSEGMENTS)) {
          totalSegments = Integer.parseInt(args[++i]);
      } else if (args[i].equals(ARGNAME_COMPRESSION)) {
        // Handled at a higher level
      }
      } catch (ArrayIndexOutOfBoundsException e) {
        LOG.error("Command line usage was incorrect -- see documentation");
      GenericOptionsParser.printGenericCommandUsage(System.out);
      throw new IllegalArgumentException();
      }
    }

    // Read in any additional config parameters.
    if (configFile != null) {
      LOG.info("adding config parameters from '"+ configFile + "'");
      this.getConf().addResource(configFile);
    }

    // Creates a new job configuration for this Hadoop job.
    Configuration conf = getConf();
    Job job = new Job(conf);
    job.setJarByClass(GALinkGraph.class);
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
        String segmentPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/"+segmentId+"/metadata-*";
        FileInputFormat.addInputPath(job, new Path(segmentPath));
      }
    }
    MaxFileFilter.setFilter(FILEFILTER);
    FileInputFormat.setInputPathFilter(job, MaxFileFilter.class);

    // Delete the output path directory if it already exists and user wants to overwrite it.
    if (overwrite) {
      LOG.info("clearing the output path at '" + outputPath + "'");
      FileSystem fs = FileSystem.get(new URI(outputPath), conf);
      if (fs.exists(new Path(outputPath))) {
        fs.delete(new Path(outputPath), true);
      }
    }

    // Set the path where final output 'part' files will be saved.
    LOG.info("setting output path to '" + outputPath + "'");
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    // Set which InputFormat class to use.
    job.setInputFormatClass(SequenceFileInputFormat.class);

    // Set which OutputFormat class to use.
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // Set which Mapper and Reducer classes to use.
    job.setMapperClass(GALinkGraph.ExportLinkGraph.class);
    if (combiner) job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return 1;
    }
  }

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
      throws Exception {

  boolean compression = false;

  for (int i = 0; i < args.length; i++) {
    if (args[i].equals(ARGNAME_COMPRESSION)) {
    compression = true;
    }
  }

  Configuration conf = new Configuration();
  if (compression) {
    // Should the whole MR job use compression?
    conf.setBoolean("mapred.output.compress", true);
    // Use Gzip for final compressed output
    conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
    // How to compress data between the map and reduce portions
    conf.setBoolean("mapred.compress.map.output", true);
    // Use Snappy for faster compression
    conf.setClass("mapred.map.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
  }
  // Allow for a portion of the input to fail without killing the job
  // NOTE: We don't expect this much to occur, but the user would intervene
  conf.setInt("mapred.max.map.failures.percent", 10);
  // Add your S3 AWS access credentials
  conf.setStrings("fs.s3n.awsAccessKeyId", "<YOUR AWS ACCESS ID>");
  conf.setStrings("fs.s3n.awsSecretAccessKey", "<YOUR AWS SECRET KEY>");
    int res = ToolRunner.run(conf, new GALinkGraph(), args);
    System.exit(res);
  }
}
