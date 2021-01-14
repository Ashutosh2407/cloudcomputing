import java.io.*;
import java.net.URI;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.google.inject.Key;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    /* ... */
    Vertex(){
    	this.id =0;
    	this.adjacent = new Vector<Long>();
    	this.centroid = 0;
    	this.depth = 0;
    }
    Vertex(long id, Vector<Long> adjacent, long centroid, short depth){
    	this.id = id;
    	this.adjacent = adjacent;
    	this.centroid = centroid;
    	this.depth = depth;
    }
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.id);
		out.writeLong(this.centroid);
		out.writeShort(this.depth);
		out.writeInt(this.adjacent.size());
		for(int i=0;i<this.adjacent.size();i++) {
			out.writeLong(this.adjacent.get(i));
		}
		
	}
	public void readFields(DataInput in) throws IOException,EOFException {
		// TODO Auto-generated method stub
		this.id = in.readLong();
		this.centroid = in.readLong();
		this.depth = in.readShort();
		this.adjacent = new Vector<Long>();
		int n = in.readInt();
		for(int i=0;i<n;i++) {
			this.adjacent.add(in.readLong());
		}
		
	}
    
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;

    public static class MapperOne extends Mapper<Object,Text,LongWritable,Vertex>{
    	public void map(Object arg0, Text arg1, Context context) throws IOException, InterruptedException{
    		String data = arg1.toString();
    		String[] values = data.split(",");
    		Vertex v = new Vertex();
    		v.id  = Long.parseLong(values[0]);
    		for(int i=1;i<values.length;i++) {
    			v.adjacent.addElement(Long.parseLong(values[i]));
    		}
    		
    		LongWritable id = new LongWritable(v.id);
    		long cen = -1;
    		if(centroids.size()<10) {
    			centroids.add(v.id);
    			cen = v.id;
    		}
    		else {
    			cen = -1;
    		}
    		context.write(id,new Vertex(v.id, v.adjacent,cen,(short)0));
    	}
    }
    
    public static class MapperTwo extends Mapper<LongWritable,Vertex,LongWritable,Vertex>{
    	public void map(LongWritable arg0, Vertex arg1, Context context) throws IOException, InterruptedException{
    		context.write(new LongWritable(arg1.id), arg1);
    		if(arg1.centroid>0) {
    			for(Long m: arg1.adjacent) {
    				context.write(new LongWritable(m),new Vertex(m,new Vector<Long>(),arg1.centroid,BFS_depth));
    			}
    		}
    	}
    }
    
    public static class ReducerOne extends Reducer<LongWritable,Vertex,LongWritable, Vertex>{
    	public void reduce(LongWritable arg0, Iterable<Vertex> arg1, Context context) throws IOException, InterruptedException{
    		short min_depth = 1000;
    		long id = arg0.get();
    		Vertex m = new Vertex(id, new Vector<Long>(),-1,(short)0);
    		for(Vertex v:arg1) {
    			if(!v.adjacent.isEmpty()) {
    				m.adjacent = v.adjacent;
    				
    			}
    			if(v.centroid>0 && v.depth<min_depth) {
    				min_depth = v.depth;
    				m.centroid = v.centroid;
    			}
    			m.depth = min_depth;
    		}
//    		While(Vertex v:arg1) {
//    			if(!v.adjacent.isEmpty()) {
//    				m.adjacent = v.adjacent;
//    				
//    			}
//    			if(v.centroid>0 && v.depth<min_depth) {
//    				min_depth = v.depth;
//    				m.centroid = v.centroid;
//    				
//    			}
//    			m.depth = min_depth;
//    		}
    		
    		context.write(arg0, m);
    	}
    }
    
    public static class MapperThree extends Mapper<LongWritable,Vertex,LongWritable,LongWritable>{
    	public void map(LongWritable arg0, Vertex arg1, Context context) throws IOException, InterruptedException{
    		context.write( new LongWritable(arg1.centroid),new LongWritable(1));
    	}
    }
    
    public static class ReducerTwo extends Reducer<LongWritable,LongWritable,LongWritable, LongWritable>{
    	public void reduce(LongWritable arg0, Iterable<LongWritable> arg1, Context context) throws IOException, InterruptedException{
    		int m = 0;
    		for(LongWritable i:arg1) {
    			m+=i.get();
    		}
    		context.write(new LongWritable(arg0.get()), new LongWritable(m));
    	}
    }

    public static void main ( String[] args ) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJobName("AsJobOne");
        
        job1.setJarByClass(GraphPartition.class);
        job1.setMapperClass(MapperOne.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/i0"));
        job1.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            Job job2 = Job.getInstance();
            job2.setJobName("AsJobTwo");
	    	job2.setJarByClass(GraphPartition.class);
            job2.setMapperClass(MapperTwo.class);
            job2.setReducerClass(ReducerOne.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(job2, new Path(args[1]+"/i"+i));
            SequenceFileOutputFormat.setOutputPath(job2, new Path(args[1]+"/i"+(i+1)));
            job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        job3.setJobName("AsJobThree");
        job3.setJarByClass(GraphPartition.class);
		job3.setMapperClass(MapperThree.class);
        job3.setReducerClass(ReducerTwo.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.waitForCompletion(true);
    }
}

