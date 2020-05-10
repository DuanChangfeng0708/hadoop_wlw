import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PubFriendstep2 {
	private static class PubFriendMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        Text k2 = new Text();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        	//����user
        	String[] line_split = v1.toString().split("\t");
        	//��ת  userΪvalue
            v2.set(line_split[0]);
            String[] others=line_split[1].split(",");
            
            //other���������Ϊkey��user��Ϊvalue
            // tom-bob jerry
            // lucy-tom john
            for(int i=0;i<others.length;i++)
            {
            	for(int j=i+1;j<others.length;j++)
            	{
            		k2.set(others[i]+"-"+others[j]);
            		context.write(k2, v2);
            	}
            }
            
            
        }
    }

    private static class PubFriendReducer2 extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        	StringBuffer kstr = new StringBuffer();
        	
        	//����  �����������
        	Vector<String> vals=new Vector<String>();
			for (Text text : values) {
				vals.add(text.toString());
			}
			java.util.Collections.sort(vals);
			for(String s:vals)
			{
				kstr.append(s+",");
			}
			context.write(key, new Text(kstr.toString()));
        }
    }

    
    public static boolean startStep2(String inPath,String outPath) {
    	boolean res=false;
    	try {
			//1.��ȡjob
			Job job = Job.getInstance(new Configuration());
			//2.����jar file
			job.setJarByClass(PubFriendstep1.class);
			//3.����map class
			job.setMapperClass(PubFriendMapper2.class);
			//4.����reducer class
			job.setReducerClass(PubFriendReducer2.class);
			
			//5.����map�����
			job.setMapOutputKeyClass(Text.class);
			//6����reduce �����
			job.setMapOutputValueClass(Text.class);
			
			//7.������������·��
			FileInputFormat.setInputPaths(job, new Path(inPath));
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			//8.�ύ����
			res=job.waitForCompletion(true);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
    	return res;
    }
}
