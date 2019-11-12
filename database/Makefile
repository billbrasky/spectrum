IMAGE   = spectrum/database
VERSION = $(shell cat VERSION)

ENV     = --env-file .envrc
VOLUMES = -v $(PWD)/:/var/lib/postgresql/data
PORTS   = -p 5432:5432

build:
	docker build -t $(IMAGE):$(VERSION) .

run:
	echo "docker run $(ENV) $(PORTS) $(VOLUMES) $(IMAGE):$(VERSION)"

connect:
	psql -h 127.0.0.1 -U spectrum_coffee


# -----------------------------------
# docker-compose commands
up:
	docker-compose up

down:
	docker-compose down
