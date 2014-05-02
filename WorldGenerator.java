import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.Math;
import java.util.ArrayList;

public class WorldGenerator implements Serializable
{
	private static int citys;
	private static int districts;
	private static int streets;
	private static int buildings;
	private static int residentialUnits;
	private static int users;
	
	private static ArrayList<User> userList= new ArrayList<User>();
	private static ArrayList<ResidentialUnit> residentialList = new ArrayList<ResidentialUnit>();
	
	public static void main(String[] args)
	{
		if(args.length != 1)//app exit, if no config-file exist
		{
			System.err.println("Invalid command line, exactly one argument required");
			System.exit(1);
		}
		//read config-file and write the values in the global variable
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(args[0]));
			for(int i=0;i<6;i++)
			{
				String[] parts = reader.readLine().split("=");
				switch(i)
				{
					case 0: citys=Integer.parseInt(parts[1]); break;
					case 1: districts=Integer.parseInt(parts[1]); break;
					case 2: streets=Integer.parseInt(parts[1]); break;
					case 3: buildings=Integer.parseInt(parts[1]); break;
					case 4: residentialUnits=Integer.parseInt(parts[1]); break;
					case 5: users=Integer.parseInt(parts[1]); break;
				}
			}
			System.out.println("config-file values: "+citys+"-"+districts+"-"+streets+"-"+buildings+"-"+residentialUnits+"-"+users+"\n");
		}
		catch ( Exception e ) 
		{ 
			System.err.println( e ); 
		}
		// generate the list of all ResidentialUnit with/without Users
		int tempUsers = users;// use for user id
		int totalResidentialUnits = citys*districts*streets*buildings*residentialUnits;//use for electricity id
		if( tempUsers > totalResidentialUnits)// more users as total residential units
			tempUsers=totalResidentialUnits;
		
		for(int c=0;c<citys;c++)
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
			ObjectOutputStream o1 = new ObjectOutputStream(new FileOutputStream("userList.ser",true));
			o1.writeObject(userList);
			o1.close();
			
			ObjectOutputStream o2 = new ObjectOutputStream(new FileOutputStream("residentialList.ser",true));
			o2.writeObject(residentialList);
			o2.close();
		}
		catch ( Exception e ) 
		{ 
			System.err.println( e ); 
		}
		finally
		{
			System.out.println("WorldGenerator finished!");
			System.exit( 0 );
		}
	}
}