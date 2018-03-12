FROM stain/jena-fuseki

ENV ADMIN_PASSWORD=admin
ENV JVM_ARGS=-Xmx2g

ADD target/jenafuseki-1.0.0.jar /jenafuseki/jenafuseki.jar
ADD scripts /jenafuseki/scripts

RUN mkdir -p /jenafuseki/data

WORKDIR /jenafuseki

CMD java -cp /jenafuseki/jenafuseki.jar org.hobbit.core.run.ComponentStarter eu.hobbit.mocha.systems.jenafuseki.JenaFusekiSystemAdapter
