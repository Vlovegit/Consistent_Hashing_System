import java.io.*;
import java.net.UnknownHostException;
import java.util.Scanner;

public class BootstrapUI extends Thread implements Serializable {

	BootstrapMain bootstrapmain;
	public BootstrapUI(BootstrapMain bootstrapmain) {
		
		this.bootstrapmain = bootstrapmain;
	}
	
	@Override
	public void run() {
		
		String input = "";	
		Scanner sc = new Scanner(System.in);
		do {
			System.out.print("DHT >");
			input = sc.nextLine();
			String[] cmdValue = input.split(" ");
			
			switch(cmdValue[0]) {
			
			case "lookup":
				//System.out.println("In Lookup");
				try {
					System.out.println(bootstrapmain.lookupfunc(Integer.parseInt(cmdValue[1])));
				} catch (NumberFormatException e) {
					
					e.printStackTrace();
				} catch (UnknownHostException e) {
					
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					
					e.printStackTrace();
				} catch (IOException e) {
					
					e.printStackTrace();
				}
				break;
				
			case "insert":
				try {
					bootstrapmain.insert(Integer.parseInt(cmdValue[1]), cmdValue[2]);
				} catch (NumberFormatException | IOException | ClassNotFoundException  e) {
					
					e.printStackTrace();
				}
				break;
			
			case "delete":
				try {
					bootstrapmain.delete(Integer.parseInt(cmdValue[1]));
				} catch (NumberFormatException | IOException | ClassNotFoundException e) {
		
					e.printStackTrace();
				}
				break;
				
			default:
				System.out.println("Bash: Wrong Command Entered");
			
			}
			
			
		}while(true);
		
		
		
	}
	

}
