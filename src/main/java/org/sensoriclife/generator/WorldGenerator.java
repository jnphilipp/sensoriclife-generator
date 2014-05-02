package org.sensoriclife.generator;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

public class WorldGenerator implements Serializable
{
	private int cities;
	private int districts;
	private int streets;
	private int buildings;
	private int residentialUnits;
	private int users;
	
	private ArrayList<User> userList= new ArrayList<User>();
	private ArrayList<ResidentialUnit> residentialList = new ArrayList<ResidentialUnit>();
	
	public void run() {
		this.cities = App.getIntegerProperty("cities");
		this.districts = App.getIntegerProperty("districts");
		this.streets = App.getIntegerProperty("streets");
		this.buildings = App.getIntegerProperty("buildings");
		this.residentialUnits = App.getIntegerProperty("residentialUnits");
		this.users = App.getIntegerProperty("users");


		int tempUsers = users;// use for user id
		int totalResidentialUnits = cities*districts*streets*buildings*residentialUnits;//use for electricity id
		if( tempUsers > totalResidentialUnits)// more users as total residential units
			tempUsers=totalResidentialUnits;
		
		for(int c=0;c<cities;c++)
			for(int d=0;d<districts;d++)
				for(int s=0;s<streets;s++)
					for(int b=0;b<buildings;b++)
						for(int r=0;r<residentialUnits;r++)
						{
							if(tempUsers > 0)//busy homes
							{
								userList.add( new User(tempUsers, "city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r) );
								//System.out.println("generate user: "+tempUsers+", who live in");
								residentialList.add( new ResidentialUnit(totalResidentialUnits, "city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r, (int) (Math.random()*20+1)) );
								//System.out.println("the geneated residential unit: "+totalResidentialUnits+" in "+"city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r+"\n");
								tempUsers--;
								totalResidentialUnits--;
							}
							else//empty flats
							{
								residentialList.add( new ResidentialUnit(totalResidentialUnits, "city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r, 0 ) );
								//System.out.println("geneate the empty residential unit: "+totalResidentialUnits+" in "+"city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r+"\n");
								totalResidentialUnits--;
							}
						}
		//write the lists of users and residential units as java object, finally close the world generator
		try
		{  	
			ObjectOutputStream o1 = new ObjectOutputStream(new FileOutputStream(Helpers.getUserDir() + "/data/userList.ser",true));
			o1.writeObject(userList);
			o1.close();
			
			ObjectOutputStream o2 = new ObjectOutputStream(new FileOutputStream(Helpers.getUserDir() + "/data/residentialList.ser",true));
			o2.writeObject(residentialList);
			o2.close();
		}
		catch ( Exception e ) 
		{ 
			Logger.error(WorldGenerator.class, e.toString());
		}
		finally
		{
			Logger.info("WorldGenerator finished!");
		}
	}
}