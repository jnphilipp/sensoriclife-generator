package org.sensoriclife.generator.world;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
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
 * @version 0.1.1
 */
public class WorldGenerator extends BaseRichSpout {
	private static boolean created = false;
	private SpoutOutputCollector collector;
	private Random random;
	private Map<String, String> confs;

	public WorldGenerator() {
		this.random = new Random(System.currentTimeMillis());
		this.confs = Config.toMap();
	}

	public WorldGenerator(Map<String, String> confs) {
		this.random = new Random(System.currentTimeMillis());
		this.confs = confs;
	}

	public WorldGenerator(boolean created) {
		WorldGenerator.created = created;
		this.random = new Random(System.currentTimeMillis());
		this.confs = Config.toMap();
	}

	public WorldGenerator(boolean created, Map<String, String> confs) {
		WorldGenerator.created = created;
		this.random = new Random(System.currentTimeMillis());
		this.confs = confs;
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
		else
			Utils.sleep(3600000);
	}

	private void createWorld() {
		Logger.debug(WorldGenerator.class, "Start creating world.");

		int cities = Integer.parseInt(this.confs.get("generator.cities"));
		int districts = Integer.parseInt(this.confs.get("generator.districts"));
		int streets = Integer.parseInt(this.confs.get("generator.streets"));
		int buildings = Integer.parseInt(this.confs.get("generator.buildings"));
		int residentialUnits = Integer.parseInt(this.confs.get("generator.residentialUnits"));
		int users = Integer.parseInt(this.confs.get("generator.users"));

		try {
			if ( (((this.confs.containsKey("accumulo.name") && !this.confs.get("accumulo.name").isEmpty()) || (this.confs.containsKey("accumulo.zooServers") && !this.confs.get("accumulo.zooServers").isEmpty())) || (this.confs.containsKey("accumulo.user") && !this.confs.get("accumulo.user").isEmpty())) || (this.confs.containsKey("accumulo.password") && !this.confs.get("accumulo.password").isEmpty()) )
				Accumulo.getInstance().connect(this.confs.get("accumulo.name"), this.confs.get("accumulo.zooServers"), this.confs.get("accumulo.user"), this.confs.get("accumulo.password"));
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(WorldGenerator.class, "Error while connecting to accumulo.", e.toString());
		}

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
										for ( int k = 1; k < this.random.nextInt(5) + 2; k++ ) {
											long[] heatings = new long[this.random.nextInt(11)];
											for ( int i = 0; i < heatings.length; i++ )
												heatings[i] = totalResidentialUnits+i+k;

											ResidentialUnit residentialUnit = new ResidentialUnit(totalResidentialUnits + k, totalResidentialUnits + k, totalResidentialUnits + k, heatings, (c+k)+"-"+(d+k)+"-"+(s+k)+"-"+(b+k)+"-"+(r+k), this.random.nextInt(21) + 1);

											//accumulo
											Value value = new Value(Helpers.toByteArray(residentialUnit));
											Accumulo.getInstance().addMutation(this.confs.get("generator.table_name"), ""+rowid, "residentialUnit", "", value);
											rowid++;
											user.addAddress((c+k)+"-"+(d+k)+"-"+(s+k)+"-"+(b+k)+"-"+(r+k));

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
									Accumulo.getInstance().addMutation(this.confs.get("generator.table_name"), ""+rowid, "residentialUnit", "", value);
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
									Accumulo.getInstance().addMutation(this.confs.get("generator.table_name"), ""+rowid, "residentialUnit", "", value);
									rowid++;

									//spout
									if ( this.collector != null )
										this.collector.emit(new Values("", residentialUnit.getAddress(), "", residentialUnit.getElectricityID(), residentialUnit.getHotWaterID(), residentialUnit.getColdWaterID(), residentialUnit.getHeatingIDs()));

									totalResidentialUnits--;
								}
							}

			created = true;
			Accumulo.getInstance().closeBashWriter(this.confs.get("generator.table_name"));
		}
		catch ( IOException | MutationsRejectedException | TableNotFoundException e ) {
			Logger.error(WorldGenerator.class, "Error while creating world.", e.toString());
		}
	}
}