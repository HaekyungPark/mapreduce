package com.bit2017.mapreduce.counttrigram;

import java.io.IOException;
import java.util.StringTokenizer;

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
public class CountTrigram {
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1L);
		private Text trigram = new Text();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\r\n\t,|()<> ''.:");
			if (tokenizer.countTokens() <= 2) {
				return;
			}
				String firstWord = tokenizer.nextToken();
				String secondWord = tokenizer.nextToken();

			while (tokenizer.hasMoreTokens()) {
				String thirdWord = tokenizer.nextToken();
				trigram.set(firstWord + " " + secondWord + " " + thirdWord);
				context.write(trigram, one);

				firstWord = secondWord;
				secondWord = thirdWord;
			}
		
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		Job job = new Job(conf, "CountTrigram");
		
		//1. Job Instance 초기화 작업
		job.setJarByClass(CountTrigram.class);
		
		//2. MapperClass 지정
		job.setMapperClass(MyMapper.class);
		//3. ReducerClass 지정
		job.setReducerClass(MyReducer.class);
		// Combiner
		job.setCombinerClass(MyReducer.class);
		
		//4. 출력 키 타입
		job.setOutputKeyClass( Text.class );
		//5. 출력 value 타입
		job.setOutputValueClass(LongWritable.class);
		
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
