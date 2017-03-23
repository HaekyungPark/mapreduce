package com.bit2017.mapreduce.search;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.join.JoinIdTitle;
import com.bit2017.mapreduce.topn.TopN;

public class SearchDocs {
	public static class TitleDocIdMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write( value, new Text( key + "\t" + 1 ) );
			context.getCounter("Status","Number of Title+DocID").increment(1);
		}
	}
	
	public static class DocIdCiteCountMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text( value + "\t" + 2 ));
			context.getCounter("status","DocID+Citation").increment(1);
		}
	}

	public static class JobIdTitleReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			int count = 0;
			Text k = new Text();
			Text v = new Text();
			
			for( Text value : values ){
				String info = value.toString();
				String[] tokens = info.split("\t");
				if(tokens.length == 2){
					
				}
				if("1".equals(tokens[1])){
					k.set(tokens[0] + "[" + key.toString() + "]");
				}else if ("2".equals(tokens[1])){
					v.set(tokens[0]);
				}
				
				count++;
			}
			//출력
			if( count != 2 ){
				return;
			}
			context.write(k, v);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		Job job = new Job(conf , "Join Id & Title");
		
		//Job Instance 초기화 작업
		job.setJarByClass(JoinIdTitle.class);
		
		//파라미터 저장
		final String TITLE_DOCID =args[0];
		final String DOCID_CITECOUNT = args[1];
		final String OUTPUT_DIR = args[2];
		
		if (TITLE_DOCID == null || DOCID_CITECOUNT == null || OUTPUT_DIR == null) {
			 throw new IllegalArgumentException("Missing Parameters");
			 }
		//ReducerClass 지정
		job.setReducerClass(JobIdTitleReducer.class);
		
		//입력관련
		MultipleInputs.addInputPath(job, new Path(TITLE_DOCID), KeyValueTextInputFormat.class, TitleDocIdMapper.class);
		MultipleInputs.addInputPath(job, new Path(DOCID_CITECOUNT), KeyValueTextInputFormat.class, DocIdCiteCountMapper.class);
		//출력관련
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));
		
		
		if(!job.waitForCompletion(true))
			System.exit(1);
		
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "TopN");
		
		job2.setJarByClass(TopN.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		
		job2.setMapperClass(TopN.MyMapper.class);
		job2.setReducerClass(TopN.MyReducer.class);
		
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		// input of Job2 is output of Job
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/topN"));
		job2.getConfiguration().setInt("topN", 10);
		job2.waitForCompletion(true);
		}
}
