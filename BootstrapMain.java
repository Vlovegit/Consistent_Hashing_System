import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class BootstrapMain implements Serializable  {

    static int counter = 0; 
	private static int sPortConnection; 
	static Socket socket = null;
	static Socket forwardSocket = null; 
    private static ServerSocket serverSocket; 
	static ArrayList<Integer> serverIDArray = new ArrayList<>(); 
	HashMap<Integer, String> hashmap = new HashMap<>();  

	NameServerDetails nsDetails;

	public BootstrapMain(){
		nsDetails = new NameServerDetails(0,sPortConnection);
		System.out.println(nsDetails.sPortConnection); 
		System.out.println(nsDetails.id); 
		serverIDArray.add(0);
	}
	
	String lookupfunc(int key) throws UnknownHostException, IOException, ClassNotFoundException {
		
		if(hashmap.containsKey(key)) {
			System.out.println("////>>>>Visited Server 0"  );
			return (hashmap.get(key));
		}
		if (nsDetails.sId == 0 && nsDetails.pId == 0)
		{
			System.out.println("////>>>>Visited Server 0"  );
			return "Key Not Found at Bootstrap";
		}
		if(key > nsDetails.pId)
		{
			return "Key Not Found at Bootstrap";
		}
		//Otherwise connect with the successor
		
		 forwardSocket = new Socket(nsDetails.getSuccessorAddress(), nsDetails.sPort); 
		 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());
		 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
		 fwdOutputStream.writeObject("lookup "+key);
		 fwdOutputStream.writeObject("0");
		 String value = (String) fwdInputStream.readObject();
		 String serverTrackString = (String) fwdInputStream.readObject();
		 int count = 0;
		 for(int i = 0; i < serverTrackString.length(); i++)
		 {
			 if(serverTrackString.charAt(i) == '-')
				 count++;
		 }
		 Collections.sort(serverIDArray);
			System.out.println("////>>>>Visited Server : "  );
		 for(int id : serverIDArray) {
			 if(count-1 < 0)
				 System.out.println(id);
			 else
				 System.out.print(id + "->");
				
			 count--;
			 if(count< 0)
				 break;
		 }
		 forwardSocket.close();
		return value;
		
	}
	void insert(int key, String value) throws IOException, ClassNotFoundException {
		//Condition to check if the Key should exist in the bootstrap 
		if(key > Collections.max(serverIDArray)) {
			System.out.println("////>>>>Visited Server 0"  );
			System.out.println("////>>>>Key is added at 0"  );
			hashmap.put(key,value);
		}
			
		else {
		//Otherwise connect with successor
		 forwardSocket = new Socket(nsDetails.getSuccessorAddress(), nsDetails.sPort);
		 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());
		 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
		 fwdOutputStream.writeObject("insert "+key+" "+value);
		
		 String serverTrackString = (String) fwdInputStream.readObject();
		 int count = 0;
		 for(int i = 0; i < serverTrackString.length(); i++)
		 {
			 if(serverTrackString.charAt(i) == '-')
				 count++;
		 }
		 Collections.sort(serverIDArray);
			System.out.println("////>>>> Visited Server  : "  );
		 for(int id : serverIDArray) {
			 if(count-1 < 0)
				 System.out.println(id);
			 else
				 System.out.print(id + "->");
				
			 count--;
			 if(count< 0)
				 break;
		 }
		 forwardSocket.close();
		}
	}
	void delete(int key) throws UnknownHostException, IOException, ClassNotFoundException {
		
		//if key exists in the bootstrap server then only delete
		if(key > Collections.max(serverIDArray)) {
			System.out.println("////>>>> Visited Server  0"  );
			System.out.println("////>>>> Deletion of Key  0"  );
			hashmap.remove(key);
		}
			
		else {
		 forwardSocket = new Socket(nsDetails.getSuccessorAddress(), nsDetails.sPort);
		 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());
		 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
		 fwdOutputStream.writeObject("delete "+key);

		 String serverTrackString = (String) fwdInputStream.readObject();
		 int count = 0;boolean keyNotFound = false;
		 for(int i = 0; i < serverTrackString.length(); i++)
		 {
			 if(serverTrackString.charAt(i) == '-')
				 count++;
			 else if(serverTrackString.charAt(i) == 'N')
				 keyNotFound = true;
		 }
		 if(keyNotFound)
			 System.out.println("////>>>>Key Not Found");
		 else
			 System.out.println("////>>>>Deletion Succesful");
		 Collections.sort(serverIDArray);
			System.out.print("////>>>>Visited Server : "  );
		 for(int id : serverIDArray) {
			 if(count-1 < 0)
				 System.out.println(id);
			 else
				 System.out.print(id + "->");
				
			 count--;
			 if(count< 0)
				 break;
		 } 
		forwardSocket.close();
		//otherwise check in the successor
		}
	}

	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		
	try {
	
		if(args[0].isEmpty())
		{
			System.out.print("The name of Configuration file is not provided. Please provide it as a command line arguement");
			return;
		}
		sPortConnection = 0;
		int id = 0;
    
		File configFile = new File(args[0]); 
		
		//Reading the configuration file and printing its content	
	
			Scanner scanner = new Scanner(configFile);
			id = Integer.parseInt(scanner.nextLine());
			sPortConnection = Integer.parseInt(scanner.nextLine());
			serverSocket = new ServerSocket(sPortConnection);//listening for a new Name Server
			BootstrapMain bootstrapmain = new BootstrapMain();
		    while (scanner.hasNextLine()) {
				String[] nxtline = scanner.nextLine().split(" "); 
				bootstrapmain.hashmap.put(Integer.parseInt(nxtline[0]),nxtline[1]);
		    }


			System.out.println("Contents of the Configuration file:");
			System.out.println("[");
			for (Map.Entry<Integer, String> entry : bootstrapmain.hashmap.entrySet()) {
				Integer key = entry.getKey();
				String value = entry.getValue();
				System.out.println(key + " : " + value);
			}
			System.out.println("]");
		    
		    BootstrapUI btStrapUI = new BootstrapUI(bootstrapmain);
		    btStrapUI.start();
		    int maximumServerID = 0;
		    while(true) {
		    	socket = serverSocket.accept();
		    	ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream()); 
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				String nameServerDetails = (String) inputStream.readObject();
				String[] nameServerDetailsStr = nameServerDetails.split(" ");
				System.out.println(nameServerDetails);
				int newNameServerId = 0;
				int newNSPortForListening = 0;
				String newNameServerIP = "";
				if(!nameServerDetailsStr[0].equals("updatePredecessor")) { 
					if( !nameServerDetailsStr[0].equals("updateSuccessor") && !nameServerDetailsStr[0].equals("updateMaxServerID") ) {
						newNameServerId = Integer.parseInt(nameServerDetailsStr[1]);
						newNameServerIP = nameServerDetailsStr[2];
						newNSPortForListening = Integer.parseInt(nameServerDetailsStr[3]);
					}
				
				}
				
				
				
				switch(nameServerDetailsStr[0]) {
				case "newServer":
					bootstrapmain.serverIDArray.add(newNameServerId);
					outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress());
					outputStream.writeObject(sPortConnection);
					Collections.sort(bootstrapmain.serverIDArray);
					String serverTrackString = "0";
					for(int visitedID : bootstrapmain.serverIDArray)
						if(visitedID < newNameServerId)
							serverTrackString.concat("->"+visitedID);
					
					outputStream.writeObject(serverTrackString);
					System.out.println("New Name Server Id : "+newNameServerId);
					System.out.println("New Name Server started on Port : "+newNSPortForListening);
					if(bootstrapmain.nsDetails.getSuccessorId() == 0)//initially only one server 
					{
						outputStream.writeObject(bootstrapmain.nsDetails.sPortConnection);//it's for the succssor port
						outputStream.writeObject(bootstrapmain.nsDetails.sPortConnection);//it's for the predecessor port
						outputStream.writeObject(bootstrapmain.nsDetails.id);//for the sucessor id
						outputStream.writeObject(bootstrapmain.nsDetails.id);//for the predecessor id
						outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress());//for the successor ip
						outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress());// for the predecessor ip
						bootstrapmain.nsDetails.updateInfo(newNSPortForListening, newNSPortForListening, newNameServerId, newNameServerId, newNameServerIP, newNameServerIP);
						
						//for Values from 0 to the name server id
						for(int key = 0; key < newNameServerId; key++) {
							
							if(bootstrapmain.hashmap.containsKey(key)) {
								
								outputStream.writeObject(key);
								outputStream.writeObject(bootstrapmain.hashmap.get(key));
								bootstrapmain.hashmap.remove(key);
							}
					
						}
						outputStream.writeObject(-1);
					}
					else if(maximumServerID < newNameServerId) {
						//So here bootstrap server has the keys for this name server hence, enter at last 
						
						System.out.println("////>>>>Server with largest value");
						bootstrapmain.nsDetails.pId = newNameServerId; //update the predecessor of the bootstrap
						
						int nextServerPortForListening = bootstrapmain.nsDetails.sPort;
						String nxtServerIP = bootstrapmain.nsDetails.getSuccessorAddress();
							
						forwardSocket = new Socket(nxtServerIP, nextServerPortForListening);
						 
						 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());
						 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
						 fwdOutputStream.writeObject("newServerLast "+newNameServerId + " "+ newNameServerIP + " " + newNSPortForListening); 
						 
						 int succPortForListning = (int) fwdInputStream.readObject();
						 outputStream.writeObject(succPortForListning);//sending the successor port
						 int predPortForListning = (int) fwdInputStream.readObject();
						 outputStream.writeObject(predPortForListning);//sending the predecessor port
						 int succID = (int) fwdInputStream.readObject();
						 outputStream.writeObject(succID);//sending the successor id
						 int predID = (int) fwdInputStream.readObject();
						 outputStream.writeObject(predID);//sending the predecessor id
						 String succIP = (String) fwdInputStream.readObject();	
						 outputStream.writeObject(succIP);//sending the sucessor ip
						 String predIP = (String) fwdInputStream.readObject();	
						 outputStream.writeObject(predIP);//sending the predecessor ip
						 
						 
						for(int key = maximumServerID; key < newNameServerId; key++) {
							
							if(bootstrapmain.hashmap.containsKey(key)) {
								
								outputStream.writeObject(key);
								outputStream.writeObject(bootstrapmain.hashmap.get(key));
								bootstrapmain.hashmap.remove(key);
							}
						}
						outputStream.writeObject(-1);
					}
					else {//if the new IP lies in between two servers
						
						int nxtServerPortForListening = bootstrapmain.nsDetails.sPort;
						String nxtServerIP = bootstrapmain.nsDetails.getSuccessorAddress();
						
						//The new Name Server lies between present server and the successor, updating the successor of present server
						 if(bootstrapmain.nsDetails.getSuccessorId() > newNameServerId)
							 bootstrapmain.nsDetails.updateInfo(newNSPortForListening,bootstrapmain.nsDetails.pPort, newNameServerId, bootstrapmain.nsDetails.pId, newNameServerIP,bootstrapmain.nsDetails.pAddress);
						
						//The new server does not lie in between present and successor ,and  if it does, connect with the next Name Server
						 forwardSocket = new Socket(nxtServerIP, nxtServerPortForListening);
						 
						 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());
						 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
						 fwdOutputStream.writeObject("newServer "+ newNameServerId + " " + newNameServerIP + " "+newNSPortForListening);
						 
						 int succPortForListning = (int) fwdInputStream.readObject();
						 outputStream.writeObject(succPortForListning);//sending the successor port
						 int predPortForListning = (int) fwdInputStream.readObject();
						 outputStream.writeObject(predPortForListning);//sending the predecessor port						 
						 int succId = (int) fwdInputStream.readObject();
						 outputStream.writeObject(succId);//sending the successor id
						 int predId = (int) fwdInputStream.readObject();
						 outputStream.writeObject(predId);//sending the predecessor id
						 String succIP = (String) fwdInputStream.readObject();	
						 outputStream.writeObject(succIP);//sending the successor ip
						 String predIP = (String) fwdInputStream.readObject();	
						 outputStream.writeObject(predIP);//sending the predecessor ip
						while(true) {
								
								int key =  (int) fwdInputStream.readObject();
								outputStream.writeObject(key);
								if(key == -1)
									break;
								
								String value = (String) fwdInputStream.readObject();
								outputStream.writeObject(value);
								
							}

						 forwardSocket.close();
						 System.out.println("////>>>>done");
					}
					
					
					outputStream.close();
					inputStream.close();
					socket.close();
						
					break;
					
				case "updatePredecessor":
					System.out.println("////>>>>In Successor to updatePredecessor");
					
					int predecessorPortForListning = (int) inputStream.readObject();//updating the successor port
					int predId = (int) inputStream.readObject();//updating the successor id
					String predIP = (String) inputStream.readObject();//updating the successor ip
					bootstrapmain.nsDetails.updateInfo(bootstrapmain.nsDetails.sPort, predecessorPortForListning, bootstrapmain.nsDetails.getSuccessorId(), predId,bootstrapmain.nsDetails.sAddress,predIP);
					while(true) {
						
						int key =  (int) inputStream.readObject();
						if(key == -1)
							break;
						
						String value = (String) inputStream.readObject();
						bootstrapmain.hashmap.put(key, value);
						System.out.println(">>>> "+key+" --> "+value);
						
					}
					System.out.println("Updated Information successorId" + bootstrapmain.nsDetails.sId);
					
					break;
				
				case "updateSuccessor":
					System.out.println("In Predecessor to updateSuccessor");
					int succPort = (int) inputStream.readObject();//updating the  predecessor port
					int succId = (int) inputStream.readObject();//updating the predecessor id
					String succIP = (String) inputStream.readObject();//updating the predecessor ip
					bootstrapmain.nsDetails.updateInfo(succPort, bootstrapmain.nsDetails.pPort,succId, bootstrapmain.nsDetails.pId, succIP,bootstrapmain.nsDetails.pAddress);
				break;
				
				case "updateMaxServerID":
					int exitedID = (int) inputStream.readObject();
					bootstrapmain.serverIDArray.remove(Integer.valueOf(exitedID));
					System.out.println("Updating the Max Server ID..");
					break;
				
				}
				maximumServerID = Collections.max(bootstrapmain.serverIDArray);
				System.out.println("Bootstrap server SuccessorId : "+bootstrapmain.nsDetails.sId + " PredecessorId :"+bootstrapmain.nsDetails.pId);
				

		    }
	
		} catch (FileNotFoundException e) {
		
				e.printStackTrace();
			}

	}
}
