 import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




class Point implements WritableComparable<Point> {
    public double x;
    public double y;
    
    Point()
    {
    	x=0;
    	y=0;
    }
    
    Point(double a ,double b)
    {
    	x=a;
    	y=b;
    }
	
    public void write(DataOutput out)throws IOException
    {
		out.writeDouble(x);
		out.writeDouble(y);
	}
	
	public void readFields(DataInput in) throws IOException 
	{
		// TODO Auto-generated method stub
		x=in.readDouble();
		y=in.readDouble();
	}
	
	public int compareTo(Point p) 
	{
		// TODO Auto-generated method stub
		if(x>p.x)
			return 1;
		if(x<p.x)
			return -1;
		if(y>p.y)
			return 1;
		if(y<p.y)
			return -1;
		else
			return 0;
	}
	
	public String toString() {
		return this.x+","+this.y;
	}
	
}

public class KMeans {
	 public static class AvgMapper extends Mapper<Object,Text,Point,Point> {
		 static Vector<Point> Centriods= new Vector<Point>(100); 
		 
		 public void setup(Context context)throws IOException,InterruptedException{
	    	
			URI[] paths = context.getCacheFiles();
	    	Configuration conf = context.getConfiguration();
	   		FileSystem fs = FileSystem.get(conf);
	   		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
	   		String line=reader.readLine();
	   		while(line!=null) {
	   			String[] arr=line.split(",");
	   			Point p=new Point();
	   			p.x=Double.parseDouble(arr[0]);
	   			p.y=Double.parseDouble(arr[1]);
	   			Centriods.add(p);
	   			line = reader.readLine();
	   		}
	    
		}
		
	    	
	    public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	    {
	    	//Iterator<Point> i = Centriods.iterator();
	    	String[] arr=value.toString().split(",");
   			Point p=new Point();
   			p.x=Double.parseDouble(arr[0]);
   			p.y=Double.parseDouble(arr[1]);
	    	double min=Double.MAX_VALUE;
	    	Point cent=new Point();
    		for(Point c:Centriods) {
    	    	double euc_dis = Math.sqrt((p.y - c.y) * (p.y - c.y) + (p.x - c.x) * (p.x - c.x));
	    		if(euc_dis<min) {
	    			cent=c;
	    			min=euc_dis;
	    		}
	    	
	    		
	    	}
    		context.write(cent, p);
	   	} 
		 
	    
	 }
    	

    public static class AvgReducer extends Reducer<Point,Point,Point,Object> {
    	public void reduce(Point key,Iterable<Point> value,Context context) throws IOException,InterruptedException {
    		int count=0;
    		double sx=0.0,sy=0.0;
    		for (Point val:value) {
    			count++;
    			sx=sx+val.x;
    			sy=sy+val.y;
    		}
    		key.x=sx/count;
    		key.y=sx/count;
    		context.write(key,null);
    	}
    	
    }

    public static void main ( String[] args ) throws Exception {
    	//DRIVER 
    	Job job = Job.getInstance();
        job.setJobName("Job1");
        job.setJarByClass(KMeans.class);
        job.addCacheFile(new URI(args[1]));
        
        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(Object.class);
        
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Point.class);
        
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));//(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        
        job.waitForCompletion(true);
   }
}

