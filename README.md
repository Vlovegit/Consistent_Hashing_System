# Programming project 4
## _Distributed Computing System_
### Team -13

## Group Members

Kriti Ghosh (kg23166)
email id  : kg23166@uga.edu
UGA ID: 811945814
Vaibhav Goyal (vg80700)
email id : vg80700@uga.edu
UGA ID: 811579798

## Compilation Instruction

## _Steps for running the Bootstrap_

1. ssh ugamyid@odin.cs.uga.edu  //(connect to Odin)
2. ssh ugamyid@vcf1.cs.uga.edu  //(connect to vcf )
3. cd DCS_Project4_Consistent_Hashing     //(go to project parent directory)
4. javac *.java      //(compile files)
5. java BootstrapMain <configFile>   //(run Bootstrap with configuration file as commandline argument, example: java BootstrapMain bnConfig.txt)

## _Steps for running the NameServer_

1. ssh ugamyid@odin.cs.uga.edu //(connect to Odin)
2. ssh ugamyid@vcf2.cs.uga.edu  //(connect to vcf )
3. cd DCS_Project4_Consistent_Hashing     //(go to project parent directory)
4. javac *.java     //(compile files)
5. java NameServer <configFile>     //(run Nameserver with configuration file as commandline argument, example: java BootstrapMain nsConfig.txt)

## _Commands implemented_ For BootStrap Server

1. lookup <key> :
This command obtains the value associated with a given key, provided that the key is present in the system. If the specified key is not present, the message "Key not found" is displayed. Along with the value, this command also shows the sequence of server IDs that were accessed and the ID of the server that provided the ultimate response.

2. insert <key value>
This command inserts a key-value pair into the system and displays the ID of the server where the pair was inserted. Additionally, it prints the sequence of server IDs that were accessed during the insertion process.

3. delete <key>
To delete a key-value pair associated with a given key, use this command. If the deletion is successful, it will print "Successful deletion". However, if the specified key is not present in the system, the message "Key not found" will be displayed. Moreover, the sequence of server IDs accessed during the deletion process will also be printed.


# _Commands implemented_ For Name Server

1. enter :
To join the system, the name server needs to follow a certain procedure, which starts with contacting the Bootstrap server. The name server will then determine the range of keys it should handle by applying the consistent hashing protocol, which involves moving in a clockwise direction along the existing name servers. The name server will acquire key-value pairs associated with its range from its successor name server. Upon completing the entire entry process successfully, a message saying "successful entry" will be displayed, along with the following information: (1) The range of keys that the server will manage; (2) the ID of the predecessor and successor name servers; and (3) the IDs of the servers that were accessed during the entry process.

2. exit : 
To exit the system gracefully, the name server will first inform its predecessor and successor name servers. The server will then transfer the key-value pairs it was managing to its successor. Once the exit process is successfully completed, the server will display a message saying "Successful exit". Additionally, it will print the ID of the successor and the range of keys that were handed over.

## Academic Honesty Declaration

This project was done in its entirety by Vaibhav Goyal and Kriti Ghosh. 
We hereby state that we have not received unauthorized help of any form.

