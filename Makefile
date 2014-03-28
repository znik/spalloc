CXXFLAGS=-Wall -O3 -std=c++0x -w
LIBS_PATHS=-L/usr/local/lib
LIBS=-llikwid -pthread

all:
	g++ $(CXXFLAGS) test.cpp -o test $(LIBS_PATHS) $(LIBS)


