.PHONY: up

copy:
	cp .env.example .env

copy-w:
	copy .env.example .env
	
up: 
	cd infra && docker-compose up --build --detach

down: 
	cd infra && docker-compose down

up-priv:
	cd infra && sudo docker-compose up --build