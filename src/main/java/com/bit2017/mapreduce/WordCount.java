package com.bit2017.mapreduce;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.io.NumberWritable;
import com.bit2017.mapreduce.io.StringWritable;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

public class WordCount {
	
	private static Log log = LogFactory.getLog(WordCount.class);
	
	public static class MyMapper extends Mapper<LongWritable, Text, StringWritable, NumberWritable> {
		private StringWritable word = new StringWritable();		
		private static NumberWritable one = new NumberWritable(1L);
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> map.setup() called");
		}


		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\r\n\t,|()<> ''");
			while(tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken().toLowerCase());
				context.write(word, one);
			}
		}


		@Override
		protected void cleanup(Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			log.info("--------> cleanup() called");
		}

		//run은 보통 Override하지 않는다
		/*@Override
		public void run(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			super.run(context);
		}
		*/
	}
	
	
	public static class MyReducer extends Reducer<Text, NumberWritable, Text, NumberWritable> {
		
		private NumberWritable sumWritable = new NumberWritable();
		
		@Override
		protected void reduce(Text key, Iterable<NumberWritable> values,
				Reducer<Text, NumberWritable, Text, NumberWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			for( NumberWritable value : values){
				sum += value.get();
			
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		Job job = new Job(conf , "WordCount");
		
		//1. Job Instance 초기화 작업
		job.setJarByClass(WordCount.class);
		
		//2. MapperClass 지정
		job.setMapperClass(MyMapper.class);
		//3. ReducerClass 지정
		job.setReducerClass(MyReducer.class);
		
		//4. 출력 키 타입
		job.setOutputKeyClass( StringWritable.class );
		//5. 출력 value 타입
		job.setMapOutputValueClass(NumberWritable.class);
		
		//6. 입력 파일 포멧 지정(생략 가능)
		job.setInputFormatClass(TextInputFormat.class);
		//7. 출력 파일 포멧 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//8.입력 파일 이름 저장
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//9. 출력 파일 이름 저장
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//실행.
		job.waitForCompletion(true);
	}
	}
}
