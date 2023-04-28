
import java.io.*;
import java.net.*;
import java.util.*;

public class NameServerUtils extends Thread implements Serializable  {
	
	static NameServerDetails nsDetails = null;
	NameServer nameServer = new NameServer();;
	private static ServerSocket serverSocket = null;
	static Socket socket = null;
	static Socket successorSocket = null;
	static ObjectOutputStream objectOutputStream = null;
	static ObjectInputStream objectInputStream = null;
	static HashMap<Integer, String> hashTable = new HashMap<>();
	
	public NameServerUtils(NameServer nameServer) {
		// TODO Auto-generated constructor stub
		this.nameServer = nameServer;
	}

	public void run() {
		try {
			int port = nameServer.nsDetails.sPortConnection;
			serverSocket = new ServerSocket(port);
		
		  while(true) {
		    	socket = serverSocket.accept();
		    	objectOutputStream = new  ObjectOutputStream(socket.getOutputStream());
				objectInputStream = new ObjectInputStream(socket.getInputStream());
		    	String nameServerInfo = (String) objectInputStream.readObject();
				String[] nameServerArgs = nameServerInfo.split(" ");
				System.out.println("Current command : "+nameServerArgs[0]);
				
				String successorAddress;
				int successorPort;
				switch(nameServerArgs[0]) 
				
				{
				
					case "newServer":
								//System.out.println("In serverSocket 2");
								int newNameServerId = Integer.parseInt(nameServerArgs[1]);
								String newNameServerAddress = nameServerArgs[2];
								int newNameServerPort = Integer.parseInt(nameServerArgs[3]);
								if(newNameServerId < nameServer.nsDetails.id) {
									
									System.out.println("New Name Server Id is present in between "+ nameServer.nsDetails.getPredessorId() + " and "+ nameServer.nsDetails.id);
									
									objectOutputStream.writeObject(nameServer.nsDetails.sPortConnection);// send successor port
									objectOutputStream.writeObject(nameServer.nsDetails.pPort); //send predecessor port
									objectOutputStream.writeObject(nameServer.nsDetails.id);//send successor id
									objectOutputStream.writeObject(nameServer.nsDetails.getPredessorId());//send predecessor id
									objectOutputStream.writeObject(Inet4Address.getLocalHost().getHostAddress()); //send successor address
									objectOutputStream.writeObject(nameServer.nsDetails.pAddress); //send predecessor address
									System.out.println("Transferring key to the new NS Id");
									for(int key = nameServer.nsDetails.getPredessorId(); key < newNameServerId; key++) {
										
										if(nameServer.hashTable.containsKey(key)) 
										{
											objectOutputStream.writeObject(key);
											objectOutputStream.writeObject(nameServer.hashTable.get(key));
											nameServer.hashTable.remove(key);
										}
								
									}
									nameServer.nsDetails.pId = newNameServerId;  // set predecessor id to new NSId
									nameServer.nsDetails.pAddress = newNameServerAddress; // set predecessor address to new NSId
									nameServer.nsDetails.pPort = newNameServerPort; // set predecessor port to new NSId
									objectOutputStream.writeObject(-1);
								}
								else if(newNameServerId > nameServer.nsDetails.id) {
									
									System.out.println("New Name Server Id is after "+ nameServer.nsDetails.id);

									successorPort = nameServer.nsDetails.sPort;
									successorAddress = nameServer.nsDetails.getSuccessorAddress();
									int nextSuccessorId = nameServer.nsDetails.getSuccessorId();
									
									if(nameServer.nsDetails.getSuccessorId() > newNameServerId)
										nameServer.nsDetails.updateInfo(newNameServerPort,nameServer.nsDetails.pPort, newNameServerId, nameServer.nsDetails.getPredessorId(), newNameServerAddress,nameServer.nsDetails.pAddress);
									
								
									successorSocket = new Socket(successorAddress, successorPort);
									
									ObjectInputStream successorInputStream = new ObjectInputStream(successorSocket.getInputStream());
									ObjectOutputStream successorOutputStream = new ObjectOutputStream(successorSocket.getOutputStream());
									
									successorOutputStream.writeObject("newServer "+newNameServerId + " "+ newNameServerAddress + " " + newNameServerPort);
									
									int sPort = (int) successorInputStream.readObject();
									objectOutputStream.writeObject(sPort);//send successor port
									int pPort = (int) successorInputStream.readObject();
									objectOutputStream.writeObject(pPort);//send predecessor port
									int successorId = (int) successorInputStream.readObject();
									objectOutputStream.writeObject(successorId);//send successor id
									int pId = (int) successorInputStream.readObject();
									objectOutputStream.writeObject(pId);//send predecessor id
									String successorIP = (String) successorInputStream.readObject();	
									objectOutputStream.writeObject(successorIP);//send successor address
									String pAddress = (String) successorInputStream.readObject();	
									objectOutputStream.writeObject(pAddress);//send predecessor address
									
									while(true) {
											
											int key =  (int) successorInputStream.readObject();
											objectOutputStream.writeObject(key);
											if(key == -1)
												break;
											
											String value = (String) successorInputStream.readObject();
											objectOutputStream.writeObject(value);
											
										}

									successorSocket.close();
									System.out.println("New Name Server Added.");
								}
								break;
				
					case "lookup":
										System.out.println("Lookup key in Server Id : " + nameServer.nsDetails.id);
										int key = Integer.parseInt(nameServerArgs[1]);
										String trackServer = (String) objectInputStream.readObject();
										String[] value = nameServer.lookupKeyValue(key,trackServer).split(" ");
										//System.out.println(value);	
										//System.out.println(value.length);	
										if(value.length>1)
										{
											trackServer = trackServer.concat(" -->> "+value[1]);
										}
										else
											trackServer = trackServer.concat(" -->> "+nameServer.nsDetails.id);
										objectOutputStream.writeObject(value[0]);
										objectOutputStream.writeObject(trackServer);
										break;
				
					case "insert":
										System.out.println("Insert key in Server Id : " + nameServer.nsDetails.id);
										key = Integer.parseInt(nameServerArgs[1]);
										String insertValue = nameServerArgs[2];
										String val = nameServer.insertKeyValue(key, insertValue);	
										objectOutputStream.writeObject(nameServer.nsDetails.id+" -->> "+val);
										break;
					case "delete":
										System.out.println("Delete key in Server Id : " + nameServer.nsDetails.id);
										key = Integer.parseInt(nameServerArgs[1]);
										val = nameServer.deleteKey(key);	
										objectOutputStream.writeObject(nameServer.nsDetails.id+" -->> "+val);
										break;
				
					
					
					case "newServerLast":
										System.out.println("In Server Id " + nameServer.nsDetails.id + " entering at last position");
										
										newNameServerId = Integer.parseInt(nameServerArgs[1]);
										newNameServerAddress = nameServerArgs[2];
										newNameServerPort = Integer.parseInt(nameServerArgs[3]);
										
										successorAddress = nameServer.nsDetails.getSuccessorAddress();
										successorPort = nameServer.nsDetails.sPort;
										
										if( nameServer.nsDetails.getSuccessorId() != 0 && newNameServerId > nameServer.nsDetails.id)
										{
											successorSocket = new Socket(successorAddress, successorPort);
											ObjectInputStream successorInputStream = new ObjectInputStream(successorSocket.getInputStream());
											ObjectOutputStream successorOutputStream = new ObjectOutputStream(successorSocket.getOutputStream());
											successorOutputStream.writeObject("newServerLast "+newNameServerId + " "+ newNameServerAddress + " " + newNameServerPort);
											
											successorPort = (int) successorInputStream.readObject();
											int pPort = (int) successorInputStream.readObject();
											int successorID = (int) successorInputStream.readObject();
											int pId = (int) successorInputStream.readObject();
											
											successorAddress = (String) successorInputStream.readObject();
											String pAddress = (String) successorInputStream.readObject();
											
											objectOutputStream.writeObject(successorPort);//send successor port
											objectOutputStream.writeObject(pPort);//send predecessor port
											objectOutputStream.writeObject(successorID);//send successor id
											objectOutputStream.writeObject(pId);//send predecessor id
											objectOutputStream.writeObject(successorAddress);//send successor address
											objectOutputStream.writeObject(pAddress);//send predecessor address
											
										}
										else {
											
											objectOutputStream.writeObject(successorPort);//send successor port
											objectOutputStream.writeObject(nameServer.nsDetails.pPort);//send predecessor port
											objectOutputStream.writeObject(nameServer.nsDetails.getSuccessorId());//send successor id
											objectOutputStream.writeObject(nameServer.nsDetails.id);//send predecessor id
											objectOutputStream.writeObject(successorAddress);//send successor address
											objectOutputStream.writeObject(nameServer.nsDetails.pAddress);//send predecessor address
											
											System.out.println("Successor Id for Name Server Id "+ nameServer.nsDetails.id +" is " + nameServer.nsDetails.getSuccessorId());
					
											nameServer.nsDetails.updateInfo(newNameServerPort, nameServer.nsDetails.pPort, newNameServerId, nameServer.nsDetails.getPredessorId(), newNameServerAddress, nameServer.nsDetails.pAddress);
											
										}
										break;
					
					case "updatePredecessor":
										
										System.out.println("In Successor Name Server to Update Predecessor"); // to check this
										
										int pPort = (int) objectInputStream.readObject();	//Update successor port
										int pId = (int) objectInputStream.readObject();	//Update successor id
										String pAddress = (String) objectInputStream.readObject();	//Update successor address
										nameServer.nsDetails.updateInfo(nameServer.nsDetails.sPort, pPort, nameServer.nsDetails.getSuccessorId(), pId,nameServer.nsDetails.sAddress,pAddress);
										while(true) {
											
											int key1 =  (int) objectInputStream.readObject();
											if(key1 == -1)
												break;
											
											String value1 = (String) objectInputStream.readObject();
											nameServer.hashTable.put(key1, value1);
											
										}
										System.out.println("Successfully Updated Info for Successor Id " + nameServer.nsDetails.sId);
										
										break;
				
				
					case "updateSuccessor":
										
										System.out.println("In Predessor Server to Update Successor");
										int sPort = (int) objectInputStream.readObject();	//Update predecessor port in current Nameserver
										int sId = (int) objectInputStream.readObject();	//Update predecessor id in current Nameserver
										String successorIP = (String) objectInputStream.readObject();	//Update predecessor address in current Nameserver
										nameServer.nsDetails.updateInfo(sId, nameServer.nsDetails.pPort,sId, nameServer.nsDetails.pId, successorIP, nameServer.nsDetails.pAddress);
										break;
				
				}
		    	
		    }
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
}
