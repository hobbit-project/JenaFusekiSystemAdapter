FROM java

ADD target/jenafuseki-1.0.0.jar /jenafuseki/jenafuseki.jar

WORKDIR /jenafuseki

CMD java -cp /jenafuseki/jenafuseki.jar org.hobbit.core.run.ComponentStarter eu.hobbit.mocha.systems.jenafuseki.JenaFusekiSystemAdapter