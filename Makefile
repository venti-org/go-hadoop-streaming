all: clean
	mkdir -p output/examples
	go build -o output/examples/simple ./examples/simple
	go build -o output/examples/readlines ./examples/readlines

clean:
	rm -rf output

test:
	./output/examples/simple -type complex64 < ./examples/simple/data/complex.txt
	./output/examples/simple -type int8 < ./examples/simple/data/error_int8.txt
	./output/examples/simple -type array < ./examples/simple/data/array.txt
	./output/examples/simple -type map < ./examples/simple/data/map.txt
	./output/examples/simple -type int < ./examples/simple/data/error_int.txt
	./output/examples/simple -type int -skip-err < ./examples/simple/data/error_int.txt
	./output/examples/simple -type int -key < ./examples/simple/data/error_int_key.txt
	./output/examples/simple -type int -key -skip-err < ./examples/simple/data/error_int_key.txt
