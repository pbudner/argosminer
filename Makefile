VERSION:=$(shell cat ./VERSION)
GIT_COMMIT:=$(shell git rev-list -1 HEAD)
DIST_FOLDER=./dist
BINARY=argosminer
FLAGS=-ldflags '-X "main.GitCommit=${GIT_COMMIT}" -X "main.Version=${VERSION}"'

build-ui:
	npm --prefix ui run build
	sed -i '' 's/"\/assets/"{{ .BaseURL }}\/assets/gi' ./ui/dist/index.html

build: clean build-ui
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build ${FLAGS} -o ${DIST_FOLDER}/${BINARY}-${VERSION}-darwin-amd64
	cd ${DIST_FOLDER} && tar -zcvf ${BINARY}-${VERSION}-darwin-amd64.tar.gz ${BINARY}-${VERSION}-darwin-amd64
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ${FLAGS} -o ${DIST_FOLDER}/${BINARY}-${VERSION}-linux-amd64
	cd ${DIST_FOLDER} && tar -zcvf ${BINARY}-${VERSION}-linux-amd64.tar.gz ${BINARY}-${VERSION}-linux-amd64
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ${FLAGS} -o ${DIST_FOLDER}/${BINARY}-${VERSION}-windows-amd64.exe
	cd ${DIST_FOLDER} && tar -zcvf ${BINARY}-${VERSION}-windows-amd64.tar.gz ${BINARY}-${VERSION}-windows-amd64.exe

install:
	go install ${FLAGS}

run: build
	go build ${FLAGS} -o ${DIST_FOLDER}/${BINARY}
	./${DIST_FOLDER}/${BINARY}

clean:
	
docker-build: build
	docker build . -t pbudner/argosminer:${VERSION} --build-arg VERSION=${VERSION}

docker-publish:
	docker push pbudner/argosminer:${VERSION}
	docker tag pbudner/argosminer:${VERSION} pbudner/argosminer:latest
	docker image push --all-tags pbudner/argosminer

docker: docker-build docker-publish

.PHONY: clean install
