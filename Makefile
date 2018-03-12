default: build dockerize

build:	
	mvn clean package -U -Dmaven.test.skip=true

dockerize:
	docker build -t git.project-hobbit.eu:4567/papv/jenafuseki:2.1 .

	docker push git.project-hobbit.eu:4567/papv/jenafuseki:2.1

