package tn.isima.topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, NullWritable, Text>{

private PriorityQueue<User> numberOfLikesPriorityQueue = new PriorityQueue<User>();

 
   public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
           String line = value.toString();
           String[] data = line.split(",");
           int likes = Integer.parseInt(data[9]);
           User user = likesPriorityQueue.peek();

           if (likesPriorityQueue.size()<=3 || likes > user.getLikes()) {
               likesPriorityQueue.add(new User (likes , new Text(value)));
               if (likesPriorityQueue.size()>3) {
                   likesPriorityQueue.poll();
               }
           }
   
   




while (!numberOfLikesPriorityQueue.isEmpty()){
   context.write(NullWritable.get(), likesPriorityQueue.poll().getRecord());
}
}
}
