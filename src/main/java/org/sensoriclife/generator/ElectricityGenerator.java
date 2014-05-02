package org.sensoriclife.generator;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner; 
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

public class ElectricityGenerator implements Serializable
{

	private ArrayList<User> userList= new ArrayList<User>();
	private ArrayList<ResidentialUnit> residentialList = new ArrayList<ResidentialUnit>();
	
	public void run()
	{
		//read userList & residentialList-file and write this in the global variable
		try
		{
			ObjectInputStream o1 = new ObjectInputStream(new FileInputStream(Helpers.getUserDir() + "/data/userList.ser"));
			userList = (ArrayList<User>) o1.readObject();
			o1.close();
			
			ObjectInputStream o2 = new ObjectInputStream(new FileInputStream(Helpers.getUserDir() + "/data/residentialList.ser"));
			residentialList	= (ArrayList<ResidentialUnit>) o2.readObject();
			o2.close();
		}
		catch( Exception e ) 
		{ 
			Logger.error(ElectricityGenerator.class, e.toString());
		}
		//send once a spout of the user information as jason
		
		//ask for speed
		System.out.println("0 for real time speed\n1 for fast speed");
		Scanner sc= new Scanner(System.in);
		int scan = sc.nextInt();//flag for simulation time
		int timestamp = 0;
		while(true)//endless generaed
		{
			for(int i=0;i<residentialList.size();i++)
			{
			//send spout with following data
				//id
				System.out.println( residentialList.get(i).getElectricityID() );
				//electricity meter
				double temp = residentialList.get(i).getElectricityMeter()+residentialList.get(i).getPersons()*0.2;//calculate new value with old value+persons*0,2
				residentialList.get(i).setElectricityMeter( (int)temp );//set new value
				System.out.println( residentialList.get(i).getElectricityMeter() );
				//timestamp
				System.out.println( timestamp+"\n" );
			}
			//count up timestamp
			timestamp=timestamp+15;
			//simulation time option
			if(scan == 0)
			{
				try
				{
					Thread.sleep(900000);
				}
				catch( Exception e ) 
				{ 
					Logger.error(ElectricityGenerator.class, e.toString()); 
				}
			}	
		}
	}
}
