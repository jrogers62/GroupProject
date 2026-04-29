* Here are the steps to run the database interface:
1 - unzip all the .csv files
2 - ensure that the populate.py, NHLDatabaseInterface.java, sqlite-jdbc-3.39.3.0.jar and all the .csv file are all in one directory
3 - run this to create the database from the .csv files:
  -> python3 populate.py
4 - run this to compile and run the interface:
  -> javac -cp ".:sqlite-jdbc-3.39.3.0.jar" NHLDatabaseInterface.java
  -> java -cp ".:sqlite-jdbc-3.39.3.0.jar" NHLDatabaseInterface
