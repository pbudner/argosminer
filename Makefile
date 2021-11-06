VERSION:=$(shell cat ./VERSION)
GIT_COMMIT:=$(shell git rev-list -1 HEAD)
BINARY=argosminer
FLAGS=-ldflags '-X "main.GitCommit=${GIT_COMMIT}" -X "main.Version=${VERSION}"'

hello:
	echo ${GIT_COMMIT}

build: clean
	npm --prefix ui run build
	go build ${FLAGS} -o ${BINARY}

install:
	go install ${FLAGS}

run: clean
	go build ${FLAGS} -o ${BINARY}
	./${BINARY}

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi

.PHONY: clean install
