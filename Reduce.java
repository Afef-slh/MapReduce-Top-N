package tn.isima.topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {

   private PriorityQueue<User> likesPriorityQueue = new PriorityQueue<User>();

   @Override
   public void reduce(NullWritable key , Iterable<Text> values , Context context) 
   throws IOException, InterruptedException {
	   for( Text value : values) {
       String line = value.toString();
       String[] data = line.split(",");
       int number_of_likes = Integer.parseInt(data[8]);
       User user = likesPriorityQueue.peek();

       if (likesPriorityQueue.size()<=3 || likes > user.getLikes()) {
           likesPriorityQueue.add(new User (likes , new Text(value)));
               if (likesPriorityQueue.size()>3) {
                   likesPriorityQueue.remove(user);
               }
           }
   }
	   context.write(NullWritable.get(), likesPriorityQueue.peek().getRecord());
}
   
}
