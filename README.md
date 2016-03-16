# JCGenericKafka
This a generic kafka producer-consumer java library. <br/>

It may be used to create custom producers and consumers, by extending base abstract classes. Those classes allow a faster implementation and a better customization of consumers, when compared to high level consumer class. This is mainly due to the class being based on SimpleConsumer class and offering more control over the partition where a given consumer will take data from.
<br/>

Take a look at the project <a href="https://github.com/jsoft88/JCAvroToKudu">JCAvroToKudu</a> to see how this generic kafka library was used in an application dedicated to copying data from avro files into a kudu table.
