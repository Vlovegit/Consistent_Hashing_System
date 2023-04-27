
import java.io.Serializable;

public class NameServerDetails implements Serializable {
	
	String pAddress;
	String sAddress;
	int pPort;
	int sPort;
	int sId;
	int pId;
	int id;
	int sPortConnection;

	public NameServerDetails(int id, int sPortConnection) {
		this.pId = 0;
		this.id = id;
		this.sPortConnection = sPortConnection;
		sPort = 0;
		sId = 0;
	}
	public void updateInfo(int sPort, int pPort, int sId, int pId, String sAddress, String pAddress) {
		this.sPort = sPort;
		this.sId = sId;
		this.pId = pId;
		this.pAddress = pAddress;
		this.sAddress = sAddress;
		this.pPort = pPort;
	}
	
	
	public String getSuccessorAddress() {
		return this.sAddress;
	}
	public int getSuccessorId() {
		return this.sId;
	}
	public int getPredessorId() {
		return this.pId;
	}
	public void updatePredessorId(int pId, String pAddress) {
		this.pId = pId;
		this.pAddress = pAddress;
	}
	public void updateSuccessorInfo(int sId, String sAddress) {		
		this.sId = sId;	
		this.sAddress = sAddress;
	}
	
	
}
