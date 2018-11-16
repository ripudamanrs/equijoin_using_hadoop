README
------

author - ripudaman singh 1214213311

I am using a 3 node cluster to run hadoop. 

My mapper takes a LongWritable key (row-index) and a Text value. It then outputs a list of <Text,Text> key-value pairs where the key is the column which is to be joined and the value is the entire line itself.

My reducer initializes 2 maps: one for table 1 and the other for table 2. The key of these maps are ["tablename"+"joincolumn"]. The value of these maps are the lines itself as a list.
Now the reducer uses the key to see if the current tuple is from which table and for which column value it is asking to be joined on. It will then lookup the map of the other table and see if there is a same column value present there or not, if there is, it traverses the list of values based on the map key and joins and prints with the help of ',' operator. Otherwise, it stores the current tuple in the map for which it belongs.
Therefore, the reducer returns an output of <Text,NullWritable> key-value pair (where the key is the resultant join operation itself and the value is null, since we do not need to show the result as a key value pair, i have displayed result in the key itself)

