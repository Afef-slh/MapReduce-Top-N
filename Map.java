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
           int number_of_likes = Integer.parseInt(data[9]);
           User user = numberOfLikesPriorityQueue.peek();

           if (numberOfLikesPriorityQueue.size()<=3 || number_of_likes > user.getNumberOfLikes()) {
               numberOfLikesPriorityQueue.add(new User (number_of_likes , new Text(value)));
               if (numberOfLikesPriorityQueue.size()>3) {
                   numberOfLikesPriorityQueue.poll();
               }
           }
   
   




while (!numberOfLikesPriorityQueue.isEmpty()){
   context.write(NullWritable.get(), numberOfLikesPriorityQueue.poll().getRecord());
}
}
}