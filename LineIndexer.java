import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
 
public class LineIndexer {
 
  public static class LineIndexMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, Text> {
 
    private final static Text word = new Text();
    private final static Text location = new Text();
 
    public void map(LongWritable key, Text val,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
        
      String line = val.toString();
      StringTokenizer itr = new StringTokenizer(line.toLowerCase());
     // location.set(itr.nextToken());
        
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, location);
      }
    }
  }
 
 
 
  public static class LineIndexReducer extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {
 
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
 
      boolean first = true;
      StringBuilder toReturn = new StringBuilder();
      Map <String ,Integer> fileCounter = new HashMap<String,Integer>();
     
      while (values.hasNext()){
       
          String str = values.next().toString();
          if(!fileCounter.containsKey(str)){
              fileCounter.put(str, 1);
             
          }
          else{
              int c = fileCounter.get(str);
              fileCounter.put(str,c+1);
             
          }
     
      }
    for (String id : fileCounter.keySet()){
        toReturn.append(id ).append(":").append(fileCounter.get(id)).append(" ");
       
       
    }
      output.collect(key, new Text(toReturn.toString()/*.trim())*/);
    }
  }
 

  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(LineIndexer.class);
 
    conf.setJobName("LineIndexer");
 
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
 
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
    conf.setMapperClass(LineIndexMapper.class);
    conf.setReducerClass(LineIndexReducer.class);
 
    client.setConf(conf);
 
    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}