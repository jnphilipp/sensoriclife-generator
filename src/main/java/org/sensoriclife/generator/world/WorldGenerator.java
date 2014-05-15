package org.sensoriclife.generator.world;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import org.apache.accumulo.core.data.Value;
import org.sensoriclife.Config;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul, stefan
 * @version 0.0.6
 */
public class WorldGenerator extends BaseRichSpout
{	
	private SpoutOutputCollector collector;
	private boolean worldAlreadyCreated = false;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "billing_address", "other_addresses", "electricity_id", "hotwater_id","coldwater_id", "heating_id"));
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
	
	public void createWorld() {
		int cities = Config.getIntegerProperty("cities");
		int districts = Config.getIntegerProperty("districts");
		int streets = Config.getIntegerProperty("streets");
		int buildings = Config.getIntegerProperty("buildings");
		int residentialUnits = Config.getIntegerProperty("residentialUnits");
		int users = Config.getIntegerProperty("users");

		try{
			int rowid = 1;
			int tempUsers = users;// use for user id
			if(tempUsers>100)
				tempUsers=100;
			int totalResidentialUnits = cities*districts*streets*buildings*residentialUnits;//use for electricity id
			tempUsers=tempUsers/100*totalResidentialUnits;
				

			for(int c=0;c<cities;c++)
				for(int d=0;d<districts;d++)
					for(int s=0;s<streets;s++)
						for(int b=0;b<buildings;b++)
							for(int r=0;r<residentialUnits;r++)
							{
								if(tempUsers > 0)//busy homes
								{
									if((Math.random() * 1000 + 1)==1)//one person have more than one residential unit
										tempUsers++;
									User user = new User(tempUsers, c+"-"+d+"-"+s+"-"+b+"-"+r);
									ResidentialUnit residentialUnit = new ResidentialUnit(totalResidentialUnits, totalResidentialUnits, totalResidentialUnits, totalResidentialUnits, c+"-"+d+"-"+s+"-"+b+"-"+r, (int) (Math.random()*20+1));
									//accumulo
									Value value = new Value(Helpers.toByteArray(residentialUnit));
									Accumulo.getInstance().write("generator_helper_table", ""+rowid, "residentialUnit", "", value);
									rowid++;
									//spout
									if(this.collector!=null)
										this.collector.emit(new Values(user.getName(), user.getBillingAddress(), Helpers.join(user.getOtherAddresses(), ";"), residentialUnit.getElectricityID(), residentialUnit.getHotWaterID(),residentialUnit.getColdWaterID(), residentialUnit.getHeatingID() ));

									tempUsers--;
									totalResidentialUnits--;
								}
								else//empty flats
								{
									ResidentialUnit residentialUnit = new ResidentialUnit(totalResidentialUnits, totalResidentialUnits,totalResidentialUnits,totalResidentialUnits, c+"-"+d+"-"+s+"-"+b+"-"+r, 0 ); 
									//accumulo
									Value value = new Value(Helpers.toByteArray(residentialUnit));
									Accumulo.getInstance().write("generator_helper_table", ""+rowid, "residentialUnit", "", value);
									rowid++;
									//spout
									if(this.collector!=null)
										this.collector.emit(new Values("", residentialUnit.getAddress(), "", residentialUnit.getElectricityID(), residentialUnit.getHotWaterID(), residentialUnit.getColdWaterID(), residentialUnit.getHeatingID() ));

									totalResidentialUnits--;
								}
							}
		}
		catch(Exception e)
		{
			System.err.println(e.toString());
		}
	}	
}