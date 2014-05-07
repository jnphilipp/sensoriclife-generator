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
	public static final String PATH_OUTPUT_FILE = Helpers.getUserDir() + "/data/residentialList.ser";
	
	private SpoutOutputCollector collector;
	private boolean worldAlreadyCreated = false;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("electricity"));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;		
	}

	@Override
	public void nextTuple() 
	{
		if (!worldAlreadyCreated)
			createWorld();
	}
	
	private void createWorld() {
		int cities = App.getIntegerProperty("cities");
		int districts = App.getIntegerProperty("districts");
		int streets = App.getIntegerProperty("streets");
		int buildings = App.getIntegerProperty("buildings");
		int residentialUnits = App.getIntegerProperty("residentialUnits");
		int users = App.getIntegerProperty("users");
		
		ArrayList<User> userList= new ArrayList<User>();
		ArrayList<ResidentialUnit> residentialList = new ArrayList<ResidentialUnit>();

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
								User user = new User(tempUsers, "city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r);
								userList.add(user);
								this.collector.emit(new Values(user.getID(), user.getName(), user.getAddresses()));

								ResidentialUnit residentialUnit = new ResidentialUnit(totalResidentialUnits, "city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r, (int) (Math.random()*20+1));
								residentialList.add(residentialUnit);
								this.collector.emit(new Values(residentialUnit.getElectricityID(), residentialUnit.getAddress()));
								
								tempUsers--;
								totalResidentialUnits--;
							}
							else//empty flats
							{
								ResidentialUnit residentialUnit = new ResidentialUnit(totalResidentialUnits, "city "+c+",district "+d+",street "+s+",building "+b+",residential unit "+r, 0 ); 
								residentialList.add( residentialUnit);
								this.collector.emit(new Values(residentialUnit.getElectricityID(), residentialUnit.getAddress()));
								totalResidentialUnits--;
							}
						}
		//write the lists of users and residential units as java object, finally close the world generator
		try
		{  
			ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(PATH_OUTPUT_FILE, true));
			o.writeObject(residentialList);
			o.close();
			Logger.info("World is created and written to " + PATH_OUTPUT_FILE);
			this.worldAlreadyCreated = true;
		}
		catch ( Exception e ) 
		{ 
			Logger.error(WorldGenerator.class, e.toString());
		}
	}
}