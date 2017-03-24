package com.bit2017.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StringSort {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "StringSort");

		// Job Instance 초기화 작업
		job.setJarByClass(StringSort.class);

		// MapperClass 지정
		job.setMapperClass(Mapper.class);
		// ReducerClass 지정
		job.setReducerClass(Reducer.class);

		// 맵 출력 키 타입
		job.setMapOutputKeyClass(Text.class);
		// 맵 출력 value 타입
		job.setMapOutputValueClass(Text.class);
		
		// 처리 결과 출력 키 타입(리듀스)
		job.setOutputKeyClass(Text.class);
		// 처리 결과 출력 벨류 타입(리듀스)
		job.setOutputValueClass(Text.class);
		
		// 입력 파일 포멧 지정(생략 가능)
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// 출력 파일 포멧 지정(생략 가능)
		job.setOutputFormatClass(SequenceFileOutputFormat.class );

		// 입력 파일 이름 저장
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 출력 파일 이름 저장
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		// 실행
		job.waitForCompletion(true);
	}
}
