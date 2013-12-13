Measuring the impact of Google Analytics
==============================

This Hadoop programs provides two tools: `org.cs205.ccga.GACount` and `org.cs205.ccga.GALinkGraph`.

**GACount** identifies whether a web page uses Google Analytics by analyzing the HTML source for the presence of the Google Analytics Javascript code.

**GALinkGraph** generates a domain level link graph and counts the number of links that go from DomainA to DomainB.

## Suggested development environment: Norvig VM

If you'd like to have the majority of this set up for you, or you don't have a Linux system yourself, the [Linux virtual machine created for the Norvig Web Data Science Awards](http://norvigaward.github.io/vm.html) features all the tools, frameworks, and software needed to develop and run this task.

The VM also comes with sample files from the Common Crawl dataset so you can test locally before adding the complication of Amazon EMR.

## Requirements

+ Amazon EMR Command Line Interface (CLI) requires Ruby (1.8+)
+ Java and ant if compiling the code

To install the tools needed on an Ubuntu Linux machine, you can run `sudo apt-get install ant ruby1.8`. Installing Java may be more complicated depending on your setup and is left as a task for the reader.

To install the Elastic MapReduce Command Line Interface (EMR CLI), follow the instructions given at the [Amazon EMR installation guide](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-cli-install.html).

## Compiling the code

First, if you want to run an EMR job using your code, you'll need to substitute in your own credentials. In each of `org.cs205.ccga.GACount` and `org.cs205.ccga.GALinkGraph` you will find:

    conf.setStrings("fs.s3n.awsAccessKeyId", "<PUT AWS ACCESS KEY>");
    conf.setStrings("fs.s3n.awsSecretAccessKey", "<PUT AWS SECRET KEY>");

The code for the project can be compiled by going into the base directory and running `ant`.
This will produce a *JAR* file that contains the relevant executable under `dist/lib/`.

We have provided all the needed *JAR* files such that Hadoop doesn't need to be installed on your system.
If you plan on doing any development however it is highly suggested you install Hadoop locally such that you can test your code.

## Command line options

The two programs, `org.cs205.ccga.GACount` and `org.cs205.ccga.GALinkGraph`, share some options.

+ `-out [ARG]` specifies the output path, either a local filesystem location or an S3 bucket to which you have write permission
+ `-numreducers [ARG]` allows you to set the number of reducers the MapReduce job should use
+ `-totalsegments [ARG]` allows you to state how many of the 177 segments of the Common Crawl dataset your MapReduce job should process
+ `-maxfiles [ARG]` allows you to limit the total number of files to be read
+ `-overwrite` allows you to limit the total number of files to be read

For example, if we wanted to run `GACount` on 150 files of a single segment (segment = 6377 files) of the Common Crawl dataset, we could run:

    org.cs205.ccga.GACount -out s3://cs205-ga/output/ -overwrite -totalsegments 1 -maxfiles 150

In addition to the standard command line flags, the `org.cs205.ccga.GALinkGraph` was used for the Hadoop optimization experiments and has additional flags to turn on each optimization.

+ `-combiner` turns on using a combiner / in-mapper reducer
+ `-compression` turns on Snappy compression for map output and gzip compression for reduce output

Thus, if we wanted to run `GALinkGraph` over 15 segments with a combiner step and compression, we could run:

    org.cs205.ccga.GALinkGraph -out s3://cs205-ga/output/ -overwrite -totalsegments 15 -combiner -compression

## Running jobs

To create EMR clusters and run MapReduce jobs, you have two options. You can use the GUI in the Amazon EMR console, and repeatedly copy and paste the same values in, or you can use the Amazon EMR Command Line Interface (CLI).

The CLI allows you to spin up and run jobs from your own terminal.
Below is an example of running the Link Graph code on a single segment of the Common Crawl dataset using a cluster of 5 *c1.xlarge* spot instances using a *c1.medium* spot instance as the master.

    ./elastic-mapreduce --create --alive --name "Common Crawl GA: Link Graph" \
      --instance-group MASTER --instance-count 1 --instance-type c1.medium --bid-price 0.5 \
      --instance-group CORE --instance-count 5 --instance-type c1.xlarge --bid-price 0.5 \
      --key-pair ec2_cloud_east \
      --bootstrap-action s3://elasticmapreduce/bootstrap-actions/install-ganglia \
      --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
      --args "--mapred-key-value,mapred.job.reuse.jvm.num.tasks=-1,--mapred-key-value,mapred.tasktracker.map.tasks.maximum=12" \
      --jar "s3://cs205-ga/mr_code/commoncrawl-examples-1.0.1-HM-2013-12-8.jar" --step-name "Link Graph" \
      --args "org.commoncrawl.examples.MetadataOutputSLDLinks,-out,s3://cs205-ga/output/,-overwrite,-numreducers,5,-combiner,-compression,-totalsegments,1"

Note that the command line arguments to the *JAR* ("overwrite,-numreducers,5,-combiner,-compression" and so on) are all joined together, separated by commas.

For detail on what each of these options refers to, check out the [Amazon EMR CLI documentation](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-cli-commands.html).

Once this command has been run, you should see output similar to.
If you'd like to see the continued status of the cluster, you can either run `./elastic-mapreduce --list --alive` or use the GUI in the [EMR Console](https://console.aws.amazon.com/elasticmapreduce/).

    smerity@pegasus:~/Coding/cs205_ga$ ./elastic-mapreduce-cli/elastic-mapreduce --list --active
    j-2814ESI4Q6WMI     BOOTSTRAPPING  ec2-54-196-14-162.compute-1.amazonaws.com         Common Crawl GA: Link Graph
       PENDING        Link Graph   

## Observing progress

To use the Ganglia or Hadoop job tracker web interfaces, we need to forward ports from the Hadoop master node to your local machine.

First, we need to find the address of the Hadoop master node.

    smerity@pegasus:~/Coding/cs205_ga$ ./elastic-mapreduce-cli/elastic-mapreduce --list --active
    j-1F2CSIRBN12TH     RUNNING        ec2-54-211-14-43.compute-1.amazonaws.com          Common Crawl GA: Link Graph
       RUNNING        Link Graph

The command above tells you that the master node's address is `ec2-54-211-14-43.compute-1.amazonaws.com`.

From this, we can run

    ssh -L 9100:localhost:9100 -L 8001:localhost:80 hadoop@ec2-54-211-14-43.compute-1.amazonaws.com -i ~/.ssh/amazon/ec2_cloud_east.pem

to connect to the machine. Ensure you substitute your SSH private key as appropriate.

The relevant web pages will be at [localhost:8001/ganglia/](localhost:8001/ganglia/) and [localhost:9100](localhost:9100) for Ganglia and the Hadoop job tracker respectively.

## Useful commands to run on Hadoop master

The Amazon EMR GUI does show progress logs, but does so in a slow and temperamental fashion. If you want up to the second updates on the status of the job, you either need to use Hadoop's job tracker or view the log files themselves.

    # View all of the logs files produced by Hadoop
    tail -f /mnt/var/log/hadoop/*
    # View just the progress log files
    tail -f /mnt/var/log/hadoop/steps/*/syslog

### Finding and killing a job

Occasionally you will realize you are either running the wrong job or a job has started acting temperamentally.
The Amazon EMR GUI does not provide any options to kill a job in progress.
In the worst case, this means we may need to wait for an extremely slow job to complete before we can run our next job with the proper options.

To fix this, we can kill a job by SSHing to the Hadoop master node itself.

    hadoop@domU-12-31-38-04-22-65:~$ hadoop job -list
    1 jobs currently running
    JobId	State	StartTime	UserName	Priority	SchedulingInfo
    job_201312122051_0002	4	1386881935669	hadoop	NORMAL	NA

Once we know the Job ID of the offending task, we can kill it.

    hadoop@domU-12-31-38-04-22-65:~$ hadoop job -kill job_201312122051_0002
    Killed job job_201312122051_0002

Other useful commands are listed on the [Hadoop manual command guide](http://hadoop.apache.org/docs/r1.0.4/commands_manual.html).
