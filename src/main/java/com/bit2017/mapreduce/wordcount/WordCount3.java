package com.bit2017.mapreduce.wordcount;

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

import com.bit2017.mapreduce.io.NumberWritable;
import com.bit2017.mapreduce.io.StringWritable;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

public class WordCount3 {

	private static Log log = LogFactory.getLog(WordCount.class);

	public static class MyMapper extends Mapper<Text, Text, StringWritable, NumberWritable> {
		private StringWritable word = new StringWritable();
		private static NumberWritable one = new NumberWritable(1L);

		/*@Override
		protected void setup(Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> map.setup() called");
		}*/

		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\r\n\t,|()<> ''");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().toLowerCase());
				context.write(word, one);
			}
		}

		/*@Override
		protected void cleanup(Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> cleanup() called");*/
			// run은 보통 Override하지 않는다
			/*
			 * @Override public void run(Mapper<LongWritable, Text, Text,
			 * LongWritable>.Context context) throws IOException,
			 * InterruptedException { super.run(context); }
			 
		}*/
	}


	public static class MyReducer extends Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable> {
		
		private NumberWritable sumWritable = new NumberWritable();
		
		@Override
		protected void reduce(StringWritable key, Iterable<NumberWritable> values,
				Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			for( NumberWritable value : values){
				sum += value.get();
			}
			sumWritable.set(sum);
			context.getCounter("Word Status", "Count of all Words").increment(sum);
			context.write(key, sumWritable);
		
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		Job job = new Job(conf , "WordCount");
		
		// Job Instance 초기화 작업
		job.setJarByClass(WordCount.class);
		
		// MapperClass 지정
		job.setMapperClass(MyMapper.class);
		// ReducerClass 지정
		job.setReducerClass(MyReducer.class);
		
		// 컴바이너 세팅
		job.setCombinerClass(MyReducer.class);
		// 출력 키 타입
		job.setMapOutputKeyClass( StringWritable.class );
		// 출력 value 타입
		job.setMapOutputValueClass(NumberWritable.class);
		
	
		// 입력 파일 포멧 지정(생략 가능)
		job.setInputFormatClass(KeyValueTextInputFormat .class);
		// 출력 파일 포멧 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//입력 파일 이름 저장
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 출력 파일 이름 저장
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//실행.
		job.waitForCompletion(true);
	}
}
