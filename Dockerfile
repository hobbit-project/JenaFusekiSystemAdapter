FROM stain/jena as jena
FROM stain/jena-fuseki

RUN apk add --update ruby ruby-bundler coreutils findutils && rm -rf /var/cache/apk/*

ENV ADMIN_PASSWORD=admin
ENV JVM_ARGS=-Xmx2g

ADD target/jenafuseki-1.0.0.jar /jenafuseki/jenafuseki.jar
ADD scripts /jenafuseki/scripts

COPY --from=jena /jena /jena

RUN mkdir -p /jenafuseki/data

WORKDIR /jenafuseki

CMD java -cp /jenafuseki/jenafuseki.jar org.hobbit.core.run.ComponentStarter1 eu.hobbit.mocha.systems.jenafuseki.JenaFusekiSystemAdapter
