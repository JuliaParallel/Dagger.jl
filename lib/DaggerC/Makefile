JULIA_DIR = $(shell julia --compile=min -E 'realpath(joinpath(Sys.BINDIR, ".."))')

libdaggerjl:
	gcc -O0 -g3 -ggdb -shared -o libdaggerjl.so -fPIC -I$(JULIA_DIR)/include/julia -L$(JULIA_DIR)/lib -Wl,-rpath,$(JULIA_DIR)/lib dagger.c -ljulia

simple-demo: libdaggerjl.so
	gcc -O0 -g3 -ggdb -o simple-demo -fPIC -I$(JULIA_DIR)/include/julia -L$(JULIA_DIR)/lib -Wl,-rpath,$(JULIA_DIR)/lib -I. -L. -ldaggerjl simple-demo.c -ljulia
