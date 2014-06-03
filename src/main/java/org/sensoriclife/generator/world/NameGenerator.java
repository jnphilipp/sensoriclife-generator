package org.sensoriclife.generator.world;

import java.io.Serializable;
import org.sensoriclife.Logger;

/**
 * 
 * @author paul
 * @version 0.0.2
 */
public class NameGenerator implements Serializable
{
	private static String[] vocals = {"a","e","i","o","u"};
	private static String[] consonants = {"b","c","d","f","g","h","k","l","m","n","p","q","r","s","t","v","w","x","z"};
	
	public static String getName()
	{
		//first name
		String first = "";
		for ( int i = 0; i < 3; i++ ) {
			int j = (int) (Math.random()*5+1);//number betwenn 1 and 5
			int k = (int) (Math.random()*19+1);//number betwenn 1 and 19
			
			first += vocals[j-1]+consonants[k-1];
		}
		
		//last name
		String last = "";
		for ( int i = 0; i < 3; i++) {
			int j = (int) (Math.random()*5+1);//number betwenn 1 and 5
			int k = (int) (Math.random()*19+1);//number betwenn 1 and 19
			
			last += vocals[j-1]+consonants[k-1];
		}
		
		Logger.debug(NameGenerator.class, "generated name:"+first+" "+last);
		return first+" "+last;
	}
}
