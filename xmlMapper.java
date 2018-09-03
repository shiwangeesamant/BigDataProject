import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author root
 */
public class xmlMapper   extends
    Mapper<LongWritable, Text, NullWritable, Text> {



    @Override
    public void map(LongWritable key, Text value1,Context context)

throws IOException, InterruptedException {

            String xmlString = value1.toString();
              
            SAXBuilder builder = new SAXBuilder();
            Reader in = new StringReader(xmlString);
           
            String value="";
            String value2="";
            String finalValue="";
         
            try {
            
            Document doc = builder.build(in);
            Element root = doc.getRootElement();
            
            String tag1 =root.getChild("title").getTextTrim() ;
                       
            String tag2 =root.getChild("revision").getChild("text").getTextTrim();
            value= tag1+ ","+tag2;
          
			            Pattern pat = Pattern.compile("\\[\\[([^\\[\\]|]*)[^\\[\\]]*\\]\\]");
			    		Matcher matcher = pat.matcher(value);
			    		while (matcher.find()){
			    			
			    				value2 = matcher.group(1);
			    				tag1=tag1.toLowerCase();
			    				value2=value2.toLowerCase();
			            		/*if(tag1.indexOf("category:")>=0){
			            			tag1=tag1.substring(tag1.indexOf("category:")+9);
			            		}
			            		if(value2.indexOf("category:")>=0){
			            			value2=value2.substring(tag1.indexOf("category:")+10);
			            		}*/
			            		finalValue= tag1 + "	" + value2;
			            		context.write(NullWritable.get(),new Text(finalValue));
		
			    }        
             
        } catch (JDOMException ex) {
            Logger.getLogger(xmlMapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(xmlMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        }
    }


        
    

