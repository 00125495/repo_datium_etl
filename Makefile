.PHONY: clean clean-model clean-pyc docs help init init-docker create-container start-container jupyter test lint profile clean clean-data clean-docker clean-container clean-image sync-from-source sync-to-source
.DEFAULT_GOAL := help

###########################################################################################################
## SCRIPTS
###########################################################################################################

define PRINT_HELP_PYSCRIPT
import os, re, sys

if os.environ['TARGET']:
    target = os.environ['TARGET']
    is_in_target = False
    for line in sys.stdin:
        match = re.match(r'^(?P<target>{}):(?P<dependencies>.*)?## (?P<description>.*)$$'.format(target).format(target), line)
        if match:
            print("target: %-20s" % (match.group("target")))
            if "dependencies" in match.groupdict().keys():
                print("dependencies: %-20s" % (match.group("dependencies")))
            if "description" in match.groupdict().keys():
                print("description: %-20s" % (match.group("description")))
            is_in_target = True
        elif is_in_target == True:
            match = re.match(r'^\t(.+)', line)
            if match:
                command = match.groups()
                print("command: %s" % (command))
            else:
                is_in_target = False
else:
    for line in sys.stdin:
        match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
        if match:
            target, help = match.groups()
            print("%-20s %s" % (target, help))
endef

define START_DOCKER_CONTAINER
if [ `$(DOCKER) inspect -f {{.State.Running}} $(CONTAINER_NAME)` = "false" ] ; then
        $(DOCKER) start $(CONTAINER_NAME)
fi
endef

###########################################################################################################
## VARIABLES
###########################################################################################################
export DOCKER=docker
export TARGET=
export PWD=`pwd`
export PRINT_HELP_PYSCRIPT
export START_DOCKER_CONTAINER
export PYTHONPATH=$PYTHONPATH:$(PWD)
export PROJECT_NAME=repo_datium_etl
export MODE=dev
export BASE_DOCKERFILE=docker/Dockerfile
export DOCKERFILE=$(BASE_DOCKERFILE).$(MODE)
export BASE_IMAGE_NAME=$(PROJECT_NAME)-image-base
export IMAGE_NAME=$(PROJECT_NAME)-$(MODE)-image
export CONTAINER_NAME=$(PROJECT_NAME)-$(MODE)-container
export DATA_SOURCE=Please Input data source
export JUPYTER_HOST_PORT=8888
export JUPYTER_CONTAINER_PORT=8888
export PYTHON=python3

###########################################################################################################
## ADD TARGETS SPECIFIC TO "DatiumETL"
###########################################################################################################


###########################################################################################################
## GENERAL TARGETS
###########################################################################################################

help: ## show this message
	@$(PYTHON) -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

init: init-docker## initialize repository for traning

sync-from-source: ## download data data source to local envrionment
	wget $(DATA_SOURCE) -P ./data/


init-docker: ## initialize docker image
	$(DOCKER) build -t $(BASE_IMAGE_NAME) -f $(BASE_DOCKERFILE) --build-arg UID=$(shell id -u) .
	$(DOCKER) build -t $(IMAGE_NAME) -f $(DOCKERFILE) .

init-docker-no-cache: ## initialize docker image without cache
	$(DOCKER) build --no-cache -t $(BASE_IMAGE_NAME) -f $(BASE_DOCKERFILE) --build-arg UID=$(shell id -u) .
	$(DOCKER) build --no-cache -t $(IMAGE_NAME) -f $(DOCKERFILE) .

sync-to-source: ## sync local data to data source
	echo "no sync target for url data source..."


create-container: ## create docker container
	$(DOCKER) run -it -v $(PWD):/work -p $(JUPYTER_HOST_PORT):$(JUPYTER_CONTAINER_PORT) --name $(CONTAINER_NAME) $(IMAGE_NAME)

start-container: ## start docker container
	@echo "$$START_DOCKER_CONTAINER" | $(SHELL)
	@echo "Launched $(CONTAINER_NAME)..."
	$(DOCKER) attach $(CONTAINER_NAME)

jupyter: ## start Jupyter Notebook server
	jupyter-notebook --ip=0.0.0.0 --port=${JUPYTER_CONTAINER_PORT}

test: ## run test cases in tests directory
	$(PYTHON) -m unittest discover

lint: ## check style with flake8
	flake8 repo_datium_etl
	mypy repo_datium_etl

profile: ## show profile of the project
	@echo "CONTAINER_NAME: $(CONTAINER_NAME)"
	@echo "IMAGE_NAME: $(IMAGE_NAME)"
	@echo "JUPYTER_PORT: `$(DOCKER) port $(CONTAINER_NAME)`"
	@echo "DATA_SOURE: $(DATA_SOURCE)"

clean: clean-model clean-pyc clean-docker ## remove all artifacts

clean-model: ## remove model artifacts
	rm -fr model/*

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

distclean: clean ## remove all the reproducible resources including Docker images

clean-data: ## remove files under data
	rm -fr data/*

clean-docker: clean-container clean-image ## remove Docker image and container

clean-container: ## remove Docker container
	-$(DOCKER) rm $(CONTAINER_NAME)

clean-image: ## remove Docker image
	-$(DOCKER) image rm $(IMAGE_NAME)

format:
	- black scripts
	- black $(PROJECT_NAME)
