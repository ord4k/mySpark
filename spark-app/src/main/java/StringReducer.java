import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.*;


public class StringReducer {
	
	public static String method(String str) {
	    if (str != null && str.length() > 1) {
	      str = str.substring(0, str.length()-2);
	    }
	    if (str != null && str.length() == 1) {
		      System.out.println(str);
		    }
	    return str;
	}
	
	public static List<String> splitLongString(String s)
    {
        List<String> words = new ArrayList<String>();
        int start = 0;
        for(int index = 0; index <s.length(); index++)
        {
            if(s.charAt(index) == '#')
            {
                words.add(s.substring(start, index));
                start = index;
            }
        }
        return words;
    }
	public static StringBuffer sb = new StringBuffer();

	public static void main(String[] args) {
		String path = "/home/ord4k/Documents/filename.txt";
		
		try(BufferedReader br = new BufferedReader(new FileReader(path))) {
			String line = null;
			while((line=br.readLine()) != null) {
				sb.append(line);
				
			}
		} catch (FileNotFoundException e) {
			System.out.println("error message: " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("error message: " + e.getMessage());
			e.printStackTrace();
		}
		
		List<String> sentenceList = splitLongString(sb.toString());
		System.out.println(sb.toString().length());
		
		List<String> reduced = new ArrayList<String>();
		
		for(String each: sentenceList) {
			String s = method(each);
			reduced.add(s);
		}
		System.out.println(reduced);
		System.out.println(reduced.size());
		
		
		
		Set<String> set = new HashSet<String>();
		for(String each : reduced) {
			set.add(each);
			
		}
		System.out.println("set size is:" + set.size());

	
				
	}

}
