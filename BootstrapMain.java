import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class BootstrapMain implements Serializable  {

    static int counter = 0; //count
	private static int sPortConnection; //serverPortForConnection ????
	static Socket socket = null;
	static Socket forwardSocket = null; //fwdSocket
    private static ServerSocket serverSocket; //server
	static ArrayList<Integer> serverIDArray = new ArrayList<>(); //serverIDS
	HashMap<Integer, String> hashmap = new HashMap<>();  //data

	NameServerDetails nsDetails;

	public BootstrapMain(){
		nsDetails = new NameServerDetails(0,sPortConnection);
		System.out.println(nsDetails.sPortConnection); //serverPortForConnection
		System.out.println(nsDetails.id); //id not changed
		serverIDArray.add(0);
	}
	//done
	String lookupfunc(int key) throws UnknownHostException, IOException, ClassNotFoundException {
		
		if(hashmap.containsKey(key)) {//data
			System.out.println("////>>>>Visited Server 0"  );
			return (hashmap.get(key));
		}
			
		//else contact successor
		//System.out.println(nsInfo.getSuccessorIP());
	//	System.out.println(nsInfo.successorPortListning);
		 forwardSocket = new Socket(nsDetails.getSuccessorAddress(), nsDetails.sPort); //getSuccessorIP() //successorPortListning
		 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());//inputStreamFwd
		 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
		 fwdOutputStream.writeObject("lookup "+key);
		 fwdOutputStream.writeObject("0");
		 String value = (String) fwdInputStream.readObject();
		 String serverTrackString = (String) fwdInputStream.readObject();//serverTracker
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
		//check if the key should be in bootstrap
		if(key > Collections.max(serverIDArray)) {
			System.out.println("////>>>>Visited Server 0"  );
			System.out.println("////>>>>Key is added at 0"  );
			hashmap.put(key,value);
		}
			
		else {
		//if no then contact successor
		 forwardSocket = new Socket(nsDetails.getSuccessorAddress(), nsDetails.sPort);
		 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());
		 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
		 fwdOutputStream.writeObject("Insert "+key+" "+value);
		
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
		
		//if key in bootstrap server then dekete
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
		//else check in successor
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
		
		//reading the config file 	
	
			Scanner scanner = new Scanner(configFile);
			id = Integer.parseInt(scanner.nextLine());
			sPortConnection = Integer.parseInt(scanner.nextLine());
			serverSocket = new ServerSocket(sPortConnection);//server for listing to new name server //not changed
			BootstrapMain bootstrapmain = new BootstrapMain();
		    while (scanner.hasNextLine()) {
				String[] nxtline = scanner.nextLine().split(" "); 
				bootstrapmain.hashmap.put(Integer.parseInt(nxtline[0]),nxtline[1]);
		    }
		    
		    BootstrapUI btStrapUI = new BootstrapUI(bootstrapmain);
		    btStrapUI.start();
		    int maximumServerID = 0;
		    while(true) {
		    	socket = serverSocket.accept();
		    	//System.out.println("added new NameServer");
		    	ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream()); //nt chanegd
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				String nameServerDetails = (String) inputStream.readObject();
				String[] nameServerDetailsStr = nameServerDetails.split(" ");
				System.out.println(nameServerDetails);
				int newNameServerId = 0;
				int newNSPortForListening = 0;
				String newNameServerIP = "";
				if(!nameServerDetailsStr[0].equals("updateYourPredessorAndTakeAllKeys")) { //not changed
					if( !nameServerDetailsStr[0].equals("updateYourSuccessor") && !nameServerDetailsStr[0].equals("updateMaxServerID") ) {
						newNameServerId = Integer.parseInt(nameServerDetailsStr[1]);
						newNameServerIP = nameServerDetailsStr[2];
						newNSPortForListening = Integer.parseInt(nameServerDetailsStr[3]);
						//System.out.println(newNSId + newNSIP + newNSListeningPort);
					}
				
				}
				
				
				
				switch(nameServerDetailsStr[0]) {
				case "entry":
					bootstrapmain.serverIDArray.add(newNameServerId);
					outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress());
					outputStream.writeObject(sPortConnection);
					Collections.sort(bootstrapmain.serverIDArray);
					String serverTrackString = "0";
					for(int visitedID : bootstrapmain.serverIDArray)
						if(visitedID < newNameServerId)
							serverTrackString.concat("->"+visitedID);
					
					outputStream.writeObject(serverTrackString);
					System.out.println("Hello"+bootstrapmain.nsDetails.sPortConnection);
					System.out.println("Hello"+bootstrapmain.nsDetails.id);
					if(bootstrapmain.nsDetails.getSuccessorId() == 0)//if only one server intial //not changed
					{
						outputStream.writeObject(bootstrapmain.nsDetails.sPortConnection);//succssor port
						outputStream.writeObject(bootstrapmain.nsDetails.sPortConnection);//predessor port
						outputStream.writeObject(bootstrapmain.nsDetails.id);//sucessor id
						outputStream.writeObject(bootstrapmain.nsDetails.id);//predessor id
						outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress());//successor ip
						outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress());//predessor ip
						bootstrapmain.nsDetails.updateInfo(newNSPortForListening, newNSPortForListening, newNameServerId, newNameServerId, newNameServerIP, newNameServerIP);
						
						//give all the value from 0 to id
						for(int key = 0; key < newNameServerId; key++) {
							
							if(bootstrapmain.hashmap.containsKey(key)) {
								//System.out.println(key);
								outputStream.writeObject(key);
								outputStream.writeObject(bootstrapmain.hashmap.get(key));
								bootstrapmain.hashmap.remove(key);
							}
					
						}
						outputStream.writeObject(-1);
					}
					else if(maximumServerID < newNameServerId) {//this means bootstrap server has keys for this ns i.e enter at last //not changed
						
						System.out.println("////>>>>Server with largest value");
						bootstrapmain.nsDetails.pId = newNameServerId; //update predessor of bootstrap
						
						int nextServerPortForListening = bootstrapmain.nsDetails.sPort;
						String nxtServerIP = bootstrapmain.nsDetails.getSuccessorAddress();
							
						forwardSocket = new Socket(nxtServerIP, nextServerPortForListening);
						 
						 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());
						 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
						 fwdOutputStream.writeObject("entryAtLast "+newNameServerId + " "+ newNameServerIP + " " + newNSPortForListening);
						 
						 int succPortForListning = (int) fwdInputStream.readObject();
						 outputStream.writeObject(succPortForListning);//send successor port
						 int predPortForListning = (int) fwdInputStream.readObject();
						 outputStream.writeObject(predPortForListning);//send predessor port
						 int succID = (int) fwdInputStream.readObject();
						 outputStream.writeObject(succID);//send successor id
						 int predID = (int) fwdInputStream.readObject();
						 outputStream.writeObject(predID);//send predessor id
						 String succIP = (String) fwdInputStream.readObject();	
						 outputStream.writeObject(succIP);//send sucessor ip
						 String predIP = (String) fwdInputStream.readObject();	
						 outputStream.writeObject(predIP);//send predessor ip
						 
						 
						for(int key = maximumServerID; key < newNameServerId; key++) {
							
							if(bootstrapmain.hashmap.containsKey(key)) {
								//System.out.println(key);
								outputStream.writeObject(key);
								outputStream.writeObject(bootstrapmain.hashmap.get(key));
								bootstrapmain.hashmap.remove(key);
							}
						}
						outputStream.writeObject(-1);
					}
					else {//if new ip is between two servers
						
						int nxtServerPortForListening = bootstrapmain.nsDetails.sPort;
						String nxtServerIP = bootstrapmain.nsDetails.getSuccessorAddress();
						
						//1)if new nameserver is between current server and successor and update the successor of current server
						 if(bootstrapmain.nsDetails.getSuccessorId() > newNameServerId)
							 bootstrapmain.nsDetails.updateInfo(newNSPortForListening,bootstrapmain.nsDetails.pPort, newNameServerId, bootstrapmain.nsDetails.pId, newNameServerIP,bootstrapmain.nsDetails.pAddress);
						
						//2) if new server is not in between,or even if it is between contact the next nameserver
						 forwardSocket = new Socket(nxtServerIP, nxtServerPortForListening);
						 
						 ObjectInputStream fwdInputStream = new ObjectInputStream(forwardSocket.getInputStream());
						 ObjectOutputStream fwdOutputStream = new ObjectOutputStream(forwardSocket.getOutputStream());
						 fwdOutputStream.writeObject("entry "+ newNameServerId + " " + newNameServerIP + " "+newNSPortForListening);
						 
						 int succPortForListning = (int) fwdInputStream.readObject();
						 outputStream.writeObject(succPortForListning);//send successor port
						 int predPortForListning = (int) fwdInputStream.readObject();
						 outputStream.writeObject(predPortForListning);//send predessor port						 
						 int succId = (int) fwdInputStream.readObject();
						 outputStream.writeObject(succId);//send successor id
						 int predId = (int) fwdInputStream.readObject();
						 outputStream.writeObject(predId);//send predessor id
						 String succIP = (String) fwdInputStream.readObject();	
						 outputStream.writeObject(succIP);//send successor ip
						 String predIP = (String) fwdInputStream.readObject();	
						 outputStream.writeObject(predIP);//send predessor ip
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
					
				case "updateYourPredessorAndTakeAllKeys":
					System.out.println("////>>>>In Successor to updateYourPredessorAndTakeAllKeys");
					
					int predessorPortForListning = (int) inputStream.readObject();//update successor port
					int predId = (int) inputStream.readObject();//update successor id
					String predIP = (String) inputStream.readObject();//update successor ip
					bootstrapmain.nsDetails.updateInfo(bootstrapmain.nsDetails.sPort, predessorPortForListning, bootstrapmain.nsDetails.getSuccessorId(), predId,bootstrapmain.nsDetails.sAddress,predIP);
					while(true) {
						
						int key =  (int) inputStream.readObject();
						if(key == -1)
							break;
						
						String value = (String) inputStream.readObject();
						bootstrapmain.hashmap.put(key, value);
						System.out.println("Key : "+key+" Value : "+value);
						
					}
					System.out.println("Updated Informatio successorId" + bootstrapmain.nsDetails.sId);
					
					break;
				
				case "updateYourSuccessor":
					System.out.println("In Predessor to updateYourSuccessor");
					int succPort = (int) inputStream.readObject();//update predessor port
					int succId = (int) inputStream.readObject();//update predessor id
					String succIP = (String) inputStream.readObject();//update predessor ip
					bootstrapmain.nsDetails.updateInfo(succPort, bootstrapmain.nsDetails.pPort,succId, bootstrapmain.nsDetails.pId, succIP,bootstrapmain.nsDetails.pAddress);
				break;
				
				case "updateMaxServerID":
					int exitedID = (int) inputStream.readObject();
					bootstrapmain.serverIDArray.remove(Integer.valueOf(exitedID));
					System.out.println("UpdatingMaxServerID..");
					break;
				
				}
				maximumServerID = Collections.max(bootstrapmain.serverIDArray);
				System.out.println("BOOTSTRAP SuccessorId : "+bootstrapmain.nsDetails.sId + " PredessorId :"+bootstrapmain.nsDetails.pId);
				

		    }
	
		} catch (FileNotFoundException e) {
		
				e.printStackTrace();
			}

	}
}
