include cmd/kafkatop/Makefile

.PHONY: fmt
.DEFAULT_GOAL := kafkatop

kafkatop: cmd/kafkatop/kafkatop

install:
	cp cmd/kafkatop/kafkatop ~/bin

clean:
	rm -f cmd/kafkatop/kafkatop

fmt:
	go fmt
