package org.sensoriclife.generator;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul
 * @version 0.0.2
 */
public class WorldGenerator extends BaseRichSpout implements Serializable
{
	private int cities;
	private int districts;
	private int streets;
	private int buildings;
	private int residentialUnits;
	private int users;
	private SpoutOutputCollector collector;
	
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
								nextTuple(userList.get(userList.size()-1).getID(), userList.get(userList.size()-1).getName(), userList.get(userList.size()-1).getAddresses());
								residentialList.add( new ResidentialUnit(totalResidentialUnits, "city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r, (int) (Math.random()*20+1)) );
								//System.out.println("the geneated residential unit: "+totalResidentialUnits+" in "+"city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r+"\n");
								nextTuple(residentialList.get(residentialList.size()-1).getElectricityID(), residentialList.get(residentialList.size()-1).getAddress());
								tempUsers--;
								totalResidentialUnits--;
							}
							else//empty flats
							{
								residentialList.add( new ResidentialUnit(totalResidentialUnits, "city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r, 0 ) );
								//System.out.println("geneate the empty residential unit: "+totalResidentialUnits+" in "+"city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r+"\n");
								nextTuple(residentialList.get(residentialList.size()-1).getElectricityID(), residentialList.get(residentialList.size()-1).getAddress());
								totalResidentialUnits--;
							}
						}
		//write the lists of users and residential units as java object, finally close the world generator
		try
		{  	
			ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(Helpers.getUserDir() + "/data/residentialList.ser",true));
			o.writeObject(residentialList);
			o.close();
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

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("electricity"));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple(int id, String name, ArrayList<String> address) 
	{
		this.collector.emit(new Values(id, name, address));
	}
	
	public void nextTuple(int id, String address) 
	{
		this.collector.emit(new Values(id, address));
	}
}