# Makefile

# ### Configurações ###
COMPOSE_FILE := infra/docker-compose.yml
DC           := docker-compose -f $(COMPOSE_FILE)
# ou, se você usa Docker Compose v2:
# DC         := docker compose -f $(COMPOSE_FILE)

# ### Alvos Phony ###
.PHONY: all copy down up up-priv

all: up

copy:
	cp .env.example .env

down:
	$(DC) down

up:
	$(DC) up --build --detach

up-priv:
	sudo $(DC) up --build