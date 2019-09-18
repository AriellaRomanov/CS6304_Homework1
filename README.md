# SimplePageRank

### Importing Project:
* Open “eclipse”, right click on “Package Explorer” window, click import.
* Select “Git”-> “Projects from Git” and click “next”.
* Select “clone url” and click “next”.
* Paste “https://github.com/shudipdatta/SimplePageRank.git” in the “url” textbox, Change protocol to “git”, and click “next”. 
* Choose “Import existing project” and click “finish”.

### Referencing libraries:
* Right click on project and select “build path”-> “configure build path” ->”libraries”->”add external jars”.
* Go to "cloudera" -> "git" -> "SimplePageRank" -> "lib" and select all jars and click ok.

### Input file:
* Open folder "file", you will see the input file named "PageRank.txt"

### Output jar:
* Right click on project and select "Export".
* Choose type "Java" -> "Jar file" and click "next".
* Select the export destination as "cloudera" -> "git" -> "SimplePageRank" -> "jar" and click "Finish".

### Hadoop Commands:
* hadoop fs -mkdir InputFolder                                      //to create a new input folder
* hadoop fs -copyFromLocal &lt;input file> InputFolder                  //to copy a file from local directory to hadoop environment
* hadoop fs -ls InputFolder                                          //to see the files inside "InputFolder"
* hadoop jar &lt;jar file name> &lt;class name> InputFolder OutputFolder   //running mapreduce operation
* hadoop fs -ls OutputFolder                                        //to see the files inside "OutputFolder"
* hadoop fs -cat OutputFolder/part-r-00000                          //to see the content inside "OutputFolder/part-r-00000" file
* hadoop fs -rm -r OutputFolder                                     //to remove "OutputFolder" directory and all its files
  
  
### General Information (Cloudera):

* Operating System:         Mac -> Microsoft Remote Desktop, Windows -> Default Remote Desktop, Ubuntu -> Remmina
* Machine:                  cqs-cs6304-xxx.ats.mst.edu
* User:                     cloudera
* Default Password:         stu-pass
* Change Password Command:  sudo passwd cloudera

* "Firefox already running" error solve by command:     killall -SIGTERM firefox
