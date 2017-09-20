import java.io.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class Lookup {
public static class BloomFilteringMapper extends  Mapper<Object, Text, Text, NullWritable> {
private BloomFilter filter = new BloomFilter();

@Override
protected void setup(Context context) throws IOException,InterruptedException {
Configuration conf = context.getConfiguration();
FileSystem fs = FileSystem.getLocal(conf);

Path[] files  = DistributedCache.getLocalCacheFiles(conf);

 if (files != null && files.length == 1) {
String cacheFile=files[0].toString();
 System.out.println("Reading Bloom filter from: "+ cacheFile);
 DataInputStream strm = new DataInputStream(new FileInputStream(files[0].toString()));
 filter.readFields(strm);
 strm.close();
  } else {
 throw new IOException("Bloom filter file not set in the DistributedCache.");
 }
}
@Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtil.transformXmlToMap(value.toString());
 String comment = parsed.get("Text");
 System.out.println("Entered map");
 if (comment == null) {
 return;
 }
 StringTokenizer tokenizer = new StringTokenizer(comment);
 while (tokenizer.hasMoreTokens()) {
 String cleanWord = tokenizer.nextToken().replaceAll("'", "").replaceAll("[^a-zA-Z]", " ");
 if (cleanWord.length() > 0
 && filter.membershipTest(new Key(cleanWord.getBytes()))) {
 context.write(value, NullWritable.get());
 break;
 }
 }
}
}
public static void main(String[] args) throws Exception {
System.out.println("Entering main");
Configuration conf = new Configuration();
GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
String[] otherArgs = optionParser.getRemainingArgs();
System.out.println("other Args"+  Arrays.toString(otherArgs));

if (otherArgs.length != 3) {
 System.err.println("Usage: BloomFiltering <in> <cachefile> <out>");
 System.exit(1);
}

String filePath2 = otherArgs[3] ;
Path value2 = new Path(filePath2);
FileSystem.get(value2.toUri(),conf).delete(new Path(otherArgs[3]), true);

Job job = new Job(conf, "Bloom Filtering Lookup");
job.setJarByClass(Lookup.class);
job.setMapperClass(BloomFilteringMapper.class);
job.setNumReduceTasks(0);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(NullWritable.class);
FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

DistributedCache.addCacheFile(new Path("s3n://bloomfilterone/trainingoutput/hotlist.blm").toUri(), job.getConfiguration());

System.exit(job.waitForCompletion(true) ? 0 : 1);
System.out.println("exit");
}
}