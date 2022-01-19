package tn.isima.topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {

   private PriorityQueue<User> numberOfLikesPriorityQueue = new PriorityQueue<User>();

   @Override
   public void reduce(NullWritable key , Iterable<Text> values , Context context) 
   throws IOException, InterruptedException {
	   for( Text value : values) {
       String line = value.toString();
       String[] data = line.split(",");
       int number_of_likes = Integer.parseInt(data[8]);
       User user = numberOfLikesPriorityQueue.peek();

       if (numberOfLikesPriorityQueue.size()<=3 || number_of_likes > user.getNumberOfLikes()) {
           numberOfLikesPriorityQueue.add(new User (number_of_likes , new Text(value)));
               if (numberOfLikesPriorityQueue.size()>3) {
                   numberOfLikesPriorityQueue.remove(user);
               }
           }
   }
	   
}
   
}
