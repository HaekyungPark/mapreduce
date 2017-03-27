package com.bit2017.mapreduce.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CreateESIndex {
	public static class ESIndexMapper extends Mapper<Text, Text, Text, Text> {
		private String baseURL = "";
				
		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			String[] hosts = context.getConfiguration().getStrings("ESServer");
			baseURL = "http://" +hosts[0] +":9200/wikipedia/doc/";
		}
		@Override
		protected void map(Text docId, Text contents, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//URLConnection 객체 생성(연결)
			URL url = new URL(baseURL + docId);
			HttpURLConnection urlCon = (HttpURLConnection) url.openConnection();
			urlCon.setDoOutput(true);
			urlCon.setRequestMethod("PUT");
			
			String line = contents.toString().replace( "\\", "\\\\" ).replace( "\"", "\\\"" );
			String json = "{\"body\":\"" + line + "\"}";
			
			//데이터 보내기
			OutputStreamWriter out = new OutputStreamWriter(urlCon.getOutputStream());
			out.write(json);
			out.close();
			
			//응답 받기
			String lines = "";
			BufferedReader br = new BufferedReader(new InputStreamReader(urlCon.getInputStream()));
			while( (line = br.readLine()) != null ){
				lines += line;
			}
			
			//결과 처리
			if(lines.indexOf("\"successful\":1,\"failed\":0") <0 ){
				//실패
				context.getCounter("Index Stats", "fail").increment(1);
			}else{
				//성공
				context.getCounter("Index Stats", "success").increment(1);
			}
		}

		

	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		Job job = new Job(conf , "Create ES Index");
		
		// Job Instance 초기화 작업
		job.setJarByClass(CreateESIndex.class);
		
		// MapperClass 지정
		job.setMapperClass(ESIndexMapper.class);
		// ReducerClass 지정
//		job.setReducerClass(MyReducer.class);
		// 리듀스 개수 지정
		job.setNumReduceTasks(0);
		// map 출력 키 타입
		job.setMapOutputKeyClass( Text.class );
		// map 출력 value 타입
		job.setMapOutputValueClass( Text.class );
		
		// 입력 파일 포멧 지정(생략 가능)
		job.setInputFormatClass(KeyValueTextInputFormat.class);
			
		// 입력 파일 이름 저장
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 출력 파일 이름 저장
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//ES server 지정
		job.getConfiguration().setStrings("ESServer", args[2]);
		
		// 실행
		job.waitForCompletion(true);
	}
}
