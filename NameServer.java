import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

public class NameServer implements Serializable  {

	static Socket socket = null;
	static Socket successorSocket = null;
	static NameServerDetails nsDetails = null;
	static ObjectOutputStream objectOutputStream = null;
	static ObjectInputStream objectInputStream = null;
	static HashMap<Integer, String> hashTable = new HashMap<>();
	

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		List<String> conf = Files.readAllLines(Paths.get(args[0]));
		
		int id = Integer.parseInt(conf.get(0));
		int listeningPort = Integer.parseInt(conf.get(1));
		String sAddress = conf.get(2).split(" ")[0];
		int sPort = Integer.parseInt(conf.get(2).split(" ")[1]);
		String command = "";
		String bAddress = "";
		int bPort = 0;
		Scanner sc = new Scanner(System.in);
		NameServer nameServer = new NameServer();
		nameServer.nsDetails = new NameServerDetails(id, listeningPort);
		do {
			System.out.print("NameServer " + id + " >> ");
			command = sc.nextLine();
			String[] cmdArgs = command.split(" ");
			
			switch(cmdArgs[0]) {
			
			case "enter":
							socket = new Socket(sAddress, sPort);	
							objectOutputStream = new  ObjectOutputStream(socket.getOutputStream());
							objectInputStream = new ObjectInputStream(socket.getInputStream());
							String nameServerIP = Inet4Address.getLocalHost().getHostAddress();
							objectOutputStream.writeObject("newServer "+id + " "+ nameServerIP + " " + listeningPort);
							
							bAddress = (String) objectInputStream.readObject();
							bPort = (int) objectInputStream.readObject();
							String trackServer = (String) objectInputStream.readObject();
							int successorPortListning = (int) objectInputStream.readObject();
							int predessorPortListning = (int) objectInputStream.readObject();
							int successorId = (int) objectInputStream.readObject();
							int predessorId = (int) objectInputStream.readObject();
							String successorIP = (String) objectInputStream.readObject();	
							String predessorIP = (String) objectInputStream.readObject();
							
							nameServer.nsDetails.updateInfo(successorPortListning,predessorPortListning, successorId, predessorId, successorIP, predessorIP);
							nameServer.nsDetails.id = id;
							//System.out.println("SuccessorId : " + successorId +" PredessorId " +predessorId + "PredessorIP " + predessorIP+" PredessorPort : "+predessorPortListning);
							while(true) {
								
								int key =  (int) objectInputStream.readObject();
								if(key == -1)
									break;
								
								String value = (String) objectInputStream.readObject();
								nameServer.hashTable.put(key, value);
							}
							objectOutputStream.close();
							objectInputStream.close();
							socket.close();
							NameServerUtils thread = new NameServerUtils(nameServer);
							thread.start();
							System.out.println("Name Server Added Successfully");
							System.out.println("Name Server "+ id + " manages range from "+ predessorId +" to "+id);
							System.out.println("[");
							for (Map.Entry<Integer, String> entry : hashTable.entrySet()) {
								Integer key = entry.getKey();
								String value = entry.getValue();
								System.out.println(key + " : " + value);
							}
							System.out.println("]");
							System.out.println("Servers in line " + trackServer);
							break;
			
			case "exit":
							System.out.println("Surrender all the keys and update the predecessor");
							socket = new Socket(nameServer.nsDetails.sAddress, nameServer.nsDetails.sPort);
							objectOutputStream = new  ObjectOutputStream(socket.getOutputStream());
							objectInputStream = new ObjectInputStream(socket.getInputStream());
							
							System.out.println("Setting predecessor details to successor of " + nameServer.nsDetails.id);
							objectOutputStream.writeObject("updatePredecessor");
							objectOutputStream.writeObject(nameServer.nsDetails.pPort);//send Predecessor Port to successor
							objectOutputStream.writeObject(nameServer.nsDetails.pId);//Send Predecessor Id to successor
							objectOutputStream.writeObject(nameServer.nsDetails.pAddress);//Send Predecessor Address to successor
							
							for(int key = nameServer.nsDetails.pId; key < nameServer.nsDetails.id; key++) {
								if(nameServer.hashTable.containsKey(key)) {
									objectOutputStream.writeObject(key);
									objectOutputStream.writeObject(nameServer.hashTable.get(key));
									nameServer.hashTable.remove(key);
									
								}
							}
							objectOutputStream.writeObject(-1);
							objectOutputStream.close();
							objectInputStream.close();
							socket.close();
							
							System.out.println("Setting successor details to predecessor of " + nameServer.nsDetails.id);
							socket = new Socket(nameServer.nsDetails.pAddress, nameServer.nsDetails.pPort);
							objectOutputStream = new  ObjectOutputStream(socket.getOutputStream());
							objectInputStream = new ObjectInputStream(socket.getInputStream());
							
							objectOutputStream.writeObject("updateSuccessor");
							objectOutputStream.writeObject(nameServer.nsDetails.sPort);//send successor port
							objectOutputStream.writeObject(nameServer.nsDetails.sId);//send successor id
							objectOutputStream.writeObject(nameServer.nsDetails.sAddress);//send successor Address
							
							objectOutputStream.close();
							objectInputStream.close();
							socket.close();
							
							socket = new Socket(bAddress, bPort);
							objectOutputStream = new  ObjectOutputStream(socket.getOutputStream());
							objectOutputStream.writeObject("updateMaxServerID");
							objectOutputStream.writeObject(id);
							socket.close();
							break;
			
			default:		System.out.println("Invalid Command Entered. Please try again");
			}

			System.out.println(" NameServer Successor Id is "+nameServer.nsDetails.sId + " and Predessor Id is "+nameServer.nsDetails.pId);
		
		}while(!command.equals("exit"));
		System.out.println("NameServer " + id + " Exited Successfully");
		System.exit(0);
	}

	String insertKeyValue(int key, String value) throws IOException, ClassNotFoundException, UnknownHostException{
			
			if(key < nsDetails.id) 
			{
				
				//Insert key in bootstrap
				System.out.println("Key inserted in bootstrap : " + key);
				hashTable.put(key,value);
				return ""+nsDetails.id;

			}
			
			else if(key > this.nsDetails.id) 
			{
					
				//if not in bootstrap then check in the successor
				System.out.println("Checking in successor as Key "+ key + " bigger than "+ this.nsDetails.id);
				successorSocket = new Socket(nsDetails.getSuccessorAddress(), nsDetails.sPort);
				ObjectInputStream successorInputStream = new ObjectInputStream(successorSocket.getInputStream());
				ObjectOutputStream successorOutputStream = new ObjectOutputStream(successorSocket.getOutputStream());
				successorOutputStream.writeObject("insert "+key+" "+value);
				successorOutputStream.writeObject(nsDetails.id);
				value = (String) successorInputStream.readObject();
				successorSocket.close();
				return value;

			}
			return null;
		}

	String deleteKey(int key) throws IOException, ClassNotFoundException {
		
		if(key < nsDetails.id)
			
			if(hashTable.containsKey(key)) {
				hashTable.remove(key);
				System.out.println("Key "+ key + " deleted in "+ nsDetails.id);
				return ""+nsDetails.id;
			}	
			else {
				System.out.println("Cannot delete as Key "+ key + " not found in the Server "+ nsDetails.id);
				return "Key Not Found";
			}
				
		else if(key > this.nsDetails.id) {
				 
				System.out.println("Checking in successor as Key "+ key + " bigger than "+ this.nsDetails.id);
				 successorSocket = new Socket(nsDetails.getSuccessorAddress(), nsDetails.sPort);
				 ObjectInputStream successorInputStream = new ObjectInputStream(successorSocket.getInputStream());
				 ObjectOutputStream successorOutputStream = new ObjectOutputStream(successorSocket.getOutputStream());
				 successorOutputStream.writeObject("delete "+key);
				 String value = (String) successorInputStream.readObject();
				 successorSocket.close();
				 return value;
				
		}
		return null;
		
	}

	String lookupKeyValue(int key,String trackServer) throws IOException, ClassNotFoundException {
			
		if(hashTable.containsKey(key))
		{
			//System.out.println("Key Found");
			System.out.println(hashTable.get(key));
			return (hashTable.get(key));
		}
		else if(key > this.nsDetails.id) {
			
			try {
				successorSocket = new Socket(nsDetails.getSuccessorAddress(), nsDetails.sPort);
			
			ObjectInputStream successorInputStream = new ObjectInputStream(successorSocket.getInputStream());
			ObjectOutputStream successorOutputStream = new ObjectOutputStream(successorSocket.getOutputStream());
			successorOutputStream.writeObject("lookup "+key);
			successorOutputStream.writeObject(trackServer);
			String value = (String) successorInputStream.readObject();
			System.out.println("Key Value Found : " + value);
			
			String ServerTracker = (String) successorInputStream.readObject();
			System.out.println("Server " + ServerTracker);
			System.out.println("Checking in Successor Server : " + nsDetails.getSuccessorId());
			successorSocket.close();
			return value+" "+ServerTracker;
			} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
		}
		return "No Key Found at Server";
		
	}
}
