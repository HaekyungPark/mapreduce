package com.bit2017.mapreduce.countcitation;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.topn.TopN;
import com.bit2017.mapreduce.topn.TopN.MyMapper;
import com.bit2017.mapreduce.topn.TopN.MyReducer;

public class CountCitation {
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
//		private Text word = new Text();
		private static LongWritable one = new LongWritable(1L);

		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, one);
		}
	}


	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
//		private LongWritable sumWritable = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0L;
			for( LongWritable value : values){
				sum += value.get();
			}
//			sumWritable.set(sum);
			context.write(key, new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		Job job = new Job(conf , "CountCitation");
		
		//1. Job Instance 초기화 작업
		job.setJarByClass(CountCitation.class);
		
		//2. MapperClass 지정
		job.setMapperClass(MyMapper.class);
		//3. ReducerClass 지정
		job.setReducerClass(MyReducer.class);
		
		//4. 출력 키 타입
		job.setMapOutputKeyClass( Text.class );
		
		//5. 출력 value 타입
		job.setMapOutputValueClass(LongWritable.class);
		
		//6. 입력 파일 포멧 지정(생략 가능)
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		//7. 출력 파일 포멧 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//8.입력 파일 이름 저장
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//9. 출력 파일 이름 저장
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//실행.
		if(!job.waitForCompletion(true))
			return;
		
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
