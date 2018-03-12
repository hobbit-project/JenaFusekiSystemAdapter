default: build dockerize

build:	
	mvn clean package -U -Dmaven.test.skip=true

dockerize:
	docker build -t git.project-hobbit.eu:4567/papv/jenafuseki:2.0 .

	docker push git.project-hobbit.eu:4567/papv/jenafuseki:2.0

