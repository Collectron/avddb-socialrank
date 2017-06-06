package gr.tuc.softnet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SocialRankDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		if(args[0].equals("init")){
			if(args.length == 4){
				funcInit(args[1], args[2], args[3]);				
				System.exit(1);	
			}else{
				System.out.println("Wrong number of args");
				System.exit(0);
			}
						
		}
		else if(args[0].equals("iter")) {
			if(args.length == 4){
				funcIter(args[1], args[2], args[3]);				
				System.exit(1);	
			}else{
				System.out.println("Wrong number of args");
				System.exit(0);
			}
				
		}
		else if(args[0].equals("diff")){
			if(args.length == 5){
				funcDiff(args[1], args[2], args[3], args[4]);					
				System.exit(1);	
			}else{
				System.out.println("Wrong number of args");
				System.exit(0);
			}
		}
		else if(args[0].equals("finish")) {
			if(args.length == 4){
				funcFinish(args[1], args[2], args[3]);				
				System.exit(1);	
			}else{
				System.out.println("Wrong number of args");
				System.exit(0);
			}
		}
		else if(args[0].equals("composite")){
			// args[0] = composite, args[1] = inputDir, args[2] = outputDir, args[3] = intermDir1
			// args[4] = intermDir2, args[5] = diffDir, args[6] = threshold, args[7] = #ofReducers
			if(args.length == 8){
				funcInit(args[1],args[3],args[7]);
				funcIter(args[3],args[4],args[7]);
				funcDiff(args[3],args[4],args[5],args[7]);
				
				while(readDiff(args[5]) > Double.parseDouble(args[6])){
					funcIter(args[4],args[3],args[7]);
					funcIter(args[3],args[4],args[7]);
					funcDiff(args[3],args[4],args[5],args[7]);
				}
				
				funcFinish(args[4],args[2],args[7]);
				System.exit(1);
			}else{
				System.out.println("Wrong number of args");
				System.exit(0);
			}
		}
		else {System.out.println("wrong");System.exit(0);}
		
		

	}
	
	public static void funcInit(String inputDir, String outputDir, String noReducers) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Path todelete2 = new Path(outputDir);
		FileSystem fs2 = FileSystem.get(URI.create(outputDir), new Configuration());
		if (fs2.exists(todelete2)){fs2.delete(todelete2, true);}
		fs2.close();
		
		Job job = new Job();
		job.setJarByClass(SocialRankDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setMapperClass(InitMapper.class);
		job.setReducerClass(InitReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(Integer.parseInt(noReducers));
		
		job.waitForCompletion(true);	
	}
	
	public static void funcIter(String inputDir, String outputDir, String noReducers) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Path todelete2 = new Path(outputDir);
		FileSystem fs2 = FileSystem.get(URI.create(outputDir), new Configuration());
		if (fs2.exists(todelete2)){fs2.delete(todelete2, true);}
		fs2.close();
		
		Job job = new Job();
		job.setJarByClass(SocialRankDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setMapperClass(IterMapper.class);
		job.setReducerClass(IterReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(Integer.parseInt(noReducers));
			
		job.waitForCompletion(true);
	}
	
	public static void funcDiff(String inputDir1, String inputDir2, String outputDir, String noReducers) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		
		Path todelete2 = new Path(outputDir);
		FileSystem fs2 = FileSystem.get(URI.create(outputDir), new Configuration());
		if (fs2.exists(todelete2)){fs2.delete(todelete2, true);}
		fs2.close();
		
		Job job = new Job();
		job.setJarByClass(SocialRankDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(inputDir1));
		FileInputFormat.addInputPath(job, new Path(inputDir2));
		FileOutputFormat.setOutputPath(job, new Path("tmpFolder"));

		job.setMapperClass(DiffMapper1.class);
		job.setReducerClass(DiffReducer1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(Integer.parseInt(noReducers));
			
		if ( job.waitForCompletion(true) ){
			Job newJob = Job.getInstance();
			newJob.setJarByClass(SocialRankDriver.class);
			
			FileInputFormat.addInputPath(newJob, new Path("tmpFolder"));
			FileOutputFormat.setOutputPath(newJob, new Path(outputDir));

			newJob.setMapperClass(DiffMapper2.class);
			newJob.setReducerClass(DiffReducer2.class);

			newJob.setMapOutputKeyClass(Text.class);
			newJob.setMapOutputValueClass(Text.class);
			newJob.setNumReduceTasks(Integer.parseInt("1"));
			
			newJob.waitForCompletion(true);
				
			Path todelete = new Path("tmpFolder");
			FileSystem fs = FileSystem.get(URI.create("tmpFolder"), new Configuration());
			if (fs.exists(todelete)){fs.delete(todelete, true);}
			fs.close();
		}
	}
	
	public static void funcFinish(String inputDir, String outputDir, String noReducers) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Path todelete2 = new Path(outputDir);
		FileSystem fs2 = FileSystem.get(URI.create(outputDir), new Configuration());
		if (fs2.exists(todelete2)){fs2.delete(todelete2, true);}
		fs2.close();
		
		Job job = new Job();
		job.setJarByClass(SocialRankDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		job.setMapperClass(FinishMapper.class);
		job.setReducerClass(FinishReducer.class);

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(Integer.parseInt(noReducers));
		
		job.waitForCompletion(true);
	}
	
	public static double readDiff(String path) throws IOException{
		
		Path pt=new Path(path);
        FileSystem fs = FileSystem.get(URI.create(path),new Configuration());
        
        FileStatus[] fss = fs.listStatus(pt);
        for (FileStatus status : fss) {
            if(status.getPath().getName().contains("part-r")){
            	FSDataInputStream diffFile = fs.open(status.getPath());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(diffFile));
				String[] diffString = rdr.readLine().split("\t");
				if(diffString!=null){
					double diff = Double.parseDouble(diffString[1]);
					rdr.close();
					return diff;
				}
				
            }
        }
        return 0.0;
	}

}


