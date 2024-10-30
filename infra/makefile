.PHONY: up

copy:
	cp .env.example .env

copy-w:
	copy .env.example .env
	
up: 
	docker-compose up --build --detach

down: 
	docker-compose down

up-priv:
	sudo docker-compose up --build