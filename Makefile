VERSION:=$(shell cat ./VERSION)
GIT_COMMIT:=$(shell git rev-list -1 HEAD)
BINARY=dist/argosminer
FLAGS=-ldflags '-X "main.GitCommit=${GIT_COMMIT}" -X "main.Version=${VERSION}"'

hello:
	echo ${GIT_COMMIT}

build: clean
	npm --prefix ui run build
	go build ${FLAGS} -o ${BINARY}-mac
	GOOS=linux GOARCH=386 CGO_ENABLED=0 go build ${FLAGS} -o ${BINARY}-linux

install:
	go install ${FLAGS}

run: build
	go build ${FLAGS} -o ${BINARY}
	./${BINARY} && npm --prefix ui run dev

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi

docker-build: build
	docker build . -t pbudner/argosminer:latest

docker-publish:
	docker push pbudner/argosminer:latest

docker: docker-build docker-publish

.PHONY: clean install
