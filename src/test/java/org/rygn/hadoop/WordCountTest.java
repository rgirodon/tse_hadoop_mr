package org.rygn.hadoop;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class WordCountTest {
	
	@Mock
	private Mapper.Context mockMapperContext;
	
	@Mock
	private Reducer.Context mockReducerContext;
	
	private WordCount.TokenizerMapper mapper;
	
	private WordCount.IntSumReducer reducer;

	@Before
	public void setup() {
		
		this.mapper = new WordCount.TokenizerMapper();
		
		this.reducer = new WordCount.IntSumReducer();
	}
	
	@Test
	public void testMapper() throws Exception {
		
		String line1 = "C'est un beau roman, c'est une belle histoire !";
        
		mapper.map(null, new Text(line1), mockMapperContext);
        		
        verify(mockMapperContext, times(2)).write(new Text("c"), new IntWritable(1));
        verify(mockMapperContext, times(2)).write(new Text("est"), new IntWritable(1));        
        verify(mockMapperContext, times(1)).write(new Text("un"), new IntWritable(1));
        verify(mockMapperContext, times(1)).write(new Text("beau"), new IntWritable(1));
        verify(mockMapperContext, times(1)).write(new Text("roman"), new IntWritable(1));
        verify(mockMapperContext, times(1)).write(new Text("une"), new IntWritable(1));
        verify(mockMapperContext, times(1)).write(new Text("belle"), new IntWritable(1));
        verify(mockMapperContext, times(1)).write(new Text("histoire"), new IntWritable(1));
	}
	
	@Test
	public void testReducer() throws Exception {
		
		List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1));
		
		reducer.reduce(new Text("test"), values, mockReducerContext);
		
		verify(mockReducerContext, times(1)).write(new Text("test"), new IntWritable(3));
	}
	
	@Test
	public void testJob() throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCount.TokenizerMapper.class);
		job.setCombinerClass(WordCount.IntSumReducer.class);
		job.setReducerClass(WordCount.IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("./src/test/resources/mr_input"));
		
		FileOutputFormat.setOutputPath(job, new Path("./target/mr_output"));
		
		job.waitForCompletion(false);
		
		assertTrue(job.isSuccessful());
		
		List<String> resultFileLines = FileUtils.readLines(new File("./target/mr_output/part-r-00000"), Charset.forName("UTF-8"));
		
		assertTrue(resultFileLines.contains("this\t1"));
		assertTrue(resultFileLines.contains("file\t1"));
		assertTrue(resultFileLines.contains("has\t1"));
		assertTrue(resultFileLines.contains("exactly\t2"));
		assertTrue(resultFileLines.contains("ten\t2"));
		assertTrue(resultFileLines.contains("words\t1"));
		assertTrue(resultFileLines.contains("in\t1"));
		assertTrue(resultFileLines.contains("it\t1"));
		assertTrue(resultFileLines.contains("yes\t1"));
		
		FileUtils.deleteDirectory(new File("./target/mr_output"));
	}
}
