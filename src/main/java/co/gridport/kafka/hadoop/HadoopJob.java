package co.gridport.kafka.hadoop;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class HadoopJob extends Configured implements Tool {

    static {
        Configuration.addDefaultResource("core-site.xml");
    }

    public int run(String[] args) throws Exception {

        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h") || cmd.getArgs().length == 0)
        {
           printHelpAndExit(options);
        }
        String hdfsPath = cmd.getArgs()[0];

        Configuration conf = getConf();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);

        if (cmd.hasOption("topics"))
        {
            conf.set("kafka.topics", cmd.getOptionValue("topics"));
            Logger.getRootLogger().info("Using topics: " + conf.get("kafka.topics"));
        }
        else
        {
            printHelpAndExit(options);
        }

        conf.set("kafka.groupid", cmd.getOptionValue("consumer-group", "dev-hadoop-loader"));
        Logger.getRootLogger().info("Registering under consumer group: " + conf.get("kafka.groupid")); 

        conf.set("kafka.zk.connect", cmd.getOptionValue("zk-connect", "localhost:9092"));

        Logger.getRootLogger().info("Using ZooKepper connection: " + conf.get("kafka.zk.connect"));

        if (cmd.getOptionValue("autooffset-reset") != null)
        {
            conf.set("kafka.watermark.reset", cmd.getOptionValue("autooffset-reset"));
            Logger.getRootLogger().info("SHOULD RESET OFFSET TO: " + conf.get("kafka.watermark.reset"));
        }

        conf.set("input.format", cmd.getOptionValue("input-format", "json"));
        Log.info("input format======",cmd.getOptionValue("input-format", "json"));
        if (!conf.get("input.format").equals("json") && !conf.get("input.format").equals("protobuf"))
        {
            printHelpAndExit(options);
        }
        Logger.getRootLogger().info("EXPECTING MESSAGE FORMAT: " + conf.get("input.format"));

        JobConf jobConf = new JobConf(conf);
        if (cmd.hasOption("remote") )
        {
            String ip = cmd.getOptionValue("remote");
            Logger.getRootLogger().info("Default file system: hdfs://" + ip + ":8020/");
            jobConf.set("fs.defaultFS", "hdfs://"+ip+":9000/");
            Logger.getRootLogger().info("Remote jobtracker: " + ip + ":8021");
            jobConf.set("mapred.job.tracker", ip+":8021");
        }
        Path jarTarget = new Path(
            getClass().getProtectionDomain().getCodeSource().getLocation()
            + "../kafka-hadoop-loader.jar"
        );
        if (new File(jarTarget.toUri() ).exists())
        {
            //running from eclipse / as maven
            jobConf.setJar(jarTarget.toUri().getPath());
            Logger.getRootLogger().info("Using target jar: " + jarTarget.toString());
        }
        else
        {
            //running from jar remotely or locally
            jobConf.setJarByClass(getClass());
            Logger.getRootLogger().info("Using parent jar: " + jobConf.getJar());
        }

        //Ready to launch  
        Job job = Job.getInstance(jobConf, "kafka.hadoop.loader");
        job.setMapperClass(HadoopJobMapper.class);
        job.setInputFormatClass(KafkaInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(KafkaOutputFormat.class);
        job.setNumReduceTasks(0);
        KafkaOutputFormat.setOutputPath(job, new Path(hdfsPath));
        
        KafkaOutputFormat.setCompressOutput(job, cmd.getOptionValue("compress-output", "on").equals("on"));
        

        Logger.getRootLogger().info("Output hdfs location: " + hdfsPath);
        Logger.getRootLogger().info("Output hdfs compression: " + KafkaOutputFormat.getCompressOutput(job));
        boolean success = job.waitForCompletion(true);
        return success ? 0: -1;
    }

    private void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "kafka-hadoop-loader.jar", options );
        System.exit(0);
    }

    @SuppressWarnings("static-access")
    private Options buildOptions() {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("topics")
                .withLongOpt("topics")
                .hasArg()
                .withDescription("kafka topics")
                .create("t"));
        options.addOption(OptionBuilder.withArgName("groupid")
                .withLongOpt("consumer-group")
                .hasArg()
                .withDescription("kafka consumer groupid")
                .create("g"));
        options.addOption(OptionBuilder.withArgName("zk")
                .withLongOpt("zk-connect")
                .hasArg()
                .withDescription("Initial zk connection string for discovery")
                .create("z"));

        options.addOption(OptionBuilder.withArgName("offset")
                .withLongOpt("offset-reset")
                .hasArg()
                .withDescription("Reset offset to start or end of the stream e.g. 'smallest' or 'largest'")
                .create("o"));

        options.addOption(OptionBuilder.withArgName("compression")
            .withLongOpt("compress-output")
            .hasArg()
            .withDescription("GZip output compression on|off")
            .create("c"));

        options.addOption(OptionBuilder.withArgName("ip_address")
                .withLongOpt("remote")
                .hasArg()
                .withDescription("Running on a remote hadoop node")
                .create("r"));

        options.addOption(OptionBuilder
                .withLongOpt("help")
                .withDescription("Show this help")
                .create("h"));  

        options.addOption(OptionBuilder.withArgName("json|protobuf|avro")
                .withLongOpt("input-format")
                .hasArg()
                .withDescription("How are the input messages formatted in the topic")
                .create("i"));
        return options;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HadoopJob(), args);
        System.exit(exitCode);
    }

}
