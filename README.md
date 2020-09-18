# csv-java-sql

This repo contains a csv to sqlite parser in java for a specific database schema. Although inflexible, the solution is efficient for the use case and robust to failure. However, the error handling is non-standard. 

The executable ms3.jar takes an absolute filepath to the csv file. The ms3.jar uses OpenJDK 11.0.8.10. An example usage exists below. 

C:\Users\Mark\AppData\Local\Programs\AdoptOpenJDK\jdk-11.0.8.10-hotspot\bin>java -jar ms3.jar C:\Users\Mark\AppData\Local\Programs\AdoptOpenJDK\jdk-11.0.8.10-hotspot\bin\ms3Interview.csv

Included are the library jars so you can build for your own targets.

An efficient and working solution was constructed first, then safe failure was added. The database(safe commits) and log files should always be accurate, for all failure modes. I made the heavy handed choice of propagating all errors to complete failure; database reset, maximum failure written to log file. This seemed appropriate since re-running the program and expecting the same result is inherently destructive. 

I intended to use Batched statements to further increase performance, but was unable to handle the single duplicate entry. 

All database entries are stored as strings and each row is unique. Parameterized queries are used, and could be updated easily. Of all the things to assume, database schema is not one of them.(If i had chosen a PK; (A,B,C))

readAndWrite() was chosen over javabeans or maps for memory efficiency. The program should be able to handle arbitrarily large files. 
