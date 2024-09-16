Design choices 
1. Blocking Vs Call Back
    One of the design choices we made was to keep the call synchronous vs similar choices made by other system such as hive thrift server and trino where client receives a token while submitting the query and use this token to poll for the result
   Since these queries are simple and work and relatively small data set we believe keeping the protocol simple has many advantages.  