package tn.isima.topN;
import org.apache.hadoop.io.Text;

public class User implements Comparable <User> {
private int likes ;
private Text record ;

public User (int number_of_likes ,  Text record){
    this.likes = likes;
    this.record = record ;
}

public int getLikes() {
    return likes;
}
public Text getRecord() {
    return record;
}
public int compareTo (User user){
    return this.likes - user.likes;
}
}
