package org.sensoriclife.generator.world;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Value;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author paul, stefan, jnphilipp
 * @version 0.0.12
 */
public class WorldGenerator extends BaseRichSpout {
	private static boolean created = false;
	private SpoutOutputCollector collector;
	private Random random;

	public WorldGenerator() {
		this.random = new Random(System.currentTimeMillis());
	}

	public WorldGenerator(boolean created) {
		WorldGenerator.created = created;
		this.random = new Random(System.currentTimeMillis());
	}

	public static boolean isCreated() {
		return created;
	}

	public static void setCreated(boolean created) {
		WorldGenerator.created = created;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "billing_address", "other_addresses", "electricity_id", "hotwater_id","coldwater_id", "heating_ids"));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if ( !created ) {
			Logger.debug(WorldGenerator.class, "Creating world.");
			this.createWorld();
			Logger.debug(WorldGenerator.class, "Finished creating world.");
		}
	}

	private void createWorld() {
		Logger.debug(WorldGenerator.class, "Start creating world.");

		int cities = Config.getIntegerProperty("generator.cities");
		int districts = Config.getIntegerProperty("generator.districts");
		int streets = Config.getIntegerProperty("generator.streets");
		int buildings = Config.getIntegerProperty("generator.buildings");
		int residentialUnits = Config.getIntegerProperty("generator.residentialUnits");
		int users = Config.getIntegerProperty("generator.users");

		try {
			int rowid = 1;
			long totalResidentialUnits = cities*districts*streets*buildings*residentialUnits;//use for electricity id
			long tempUsers = (users > 100 ? 100 : users) / 100 * totalResidentialUnits;

			for ( int c = 0; c < cities; c++ )
				for ( int d = 0; d < districts; d++ )
					for ( int s = 0; s < streets; s++ )
						for ( int b = 0; b < buildings; b++ )
							for ( int r = 0; r < residentialUnits; r++ ) {
								if ( tempUsers > 0 ) {//busy homes
									User user = new User(tempUsers, c+"-"+d+"-"+s+"-"+b+"-"+r);

									if ( this.random.nextInt() % 10000 == 2 ) {//one person have more than one residential unit
										for ( int k = 0; k < this.random.nextInt(5) + 2; k++ ) {
											long[] heatings = new long[this.random.nextInt(11)];
											for ( int i = 0; i < heatings.length; i++ )
												heatings[i] = totalResidentialUnits+i;

											ResidentialUnit residentialUnit = new ResidentialUnit(totalResidentialUnits, totalResidentialUnits, totalResidentialUnits, heatings, c+"-"+d+"-"+s+"-"+b+"-"+r, this.random.nextInt(21) + 1);

											//accumulo
											Value value = new Value(Helpers.toByteArray(residentialUnit));
											Accumulo.getInstance().addMutation(Config.getProperty("generator.table_name"), ""+rowid, "residentialUnit", "", value);
											rowid++;
											user.addAddress(c+"-"+d+"-"+s+"-"+b+"-"+r);

											if ( this.collector != null )
												this.collector.emit(new Values("", residentialUnit.getAddress(), "", residentialUnit.getElectricityID(), residentialUnit.getHotWaterID(), residentialUnit.getColdWaterID(), residentialUnit.getHeatingIDs() ));

											totalResidentialUnits--;
											tempUsers--;//one global user less, cause one has one unit more
										}
									}

									long[] heatings = new long[this.random.nextInt(11)];
									for ( int i = 0; i < heatings.length; i++ )
										heatings[i] = totalResidentialUnits+i;

									ResidentialUnit residentialUnit = new ResidentialUnit(totalResidentialUnits, totalResidentialUnits, totalResidentialUnits, heatings, c+"-"+d+"-"+s+"-"+b+"-"+r, this.random.nextInt(21) + 1);

									//accumulo
									Value value = new Value(Helpers.toByteArray(residentialUnit));
									Accumulo.getInstance().addMutation(Config.getProperty("generator.table_name"), ""+rowid, "residentialUnit", "", value);
									rowid++;

									//spout
									if ( this.collector != null )
										this.collector.emit(new Values(user.getName(), user.getBillingAddress(), Helpers.join(user.getOtherAddresses(), ";"), residentialUnit.getElectricityID(), residentialUnit.getHotWaterID(),residentialUnit.getColdWaterID(), residentialUnit.getHeatingIDs() ));

									tempUsers--;
									totalResidentialUnits--;
								}
								else {//empty flats
									long[] heatings = new long[this.random.nextInt(11)];
									for ( int i = 0; i < heatings.length; i++ )
										heatings[i] = totalResidentialUnits+i;

									ResidentialUnit residentialUnit = new ResidentialUnit(totalResidentialUnits, totalResidentialUnits,totalResidentialUnits, heatings, c+"-"+d+"-"+s+"-"+b+"-"+r, 0 ); 

									//accumulo
									Value value = new Value(Helpers.toByteArray(residentialUnit));
									Accumulo.getInstance().addMutation(Config.getProperty("generator.table_name"), ""+rowid, "residentialUnit", "", value);
									rowid++;

									//spout
									if(this.collector!=null)
										this.collector.emit(new Values("", residentialUnit.getAddress(), "", residentialUnit.getElectricityID(), residentialUnit.getHotWaterID(), residentialUnit.getColdWaterID(), residentialUnit.getHeatingIDs() ));

									totalResidentialUnits--;
								}
							}

			created = true;
			Accumulo.getInstance().closeBashWriter(Config.getProperty("generator.table_name"));
		}
		catch ( IOException | MutationsRejectedException | TableNotFoundException e ) {
			Logger.error(WorldGenerator.class, "Error while creating world.", e.toString());
		}
	}
}