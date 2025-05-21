CC = gcc
CFLAGS = -Wall -pthread

# Compile each source file into an object file
threadpool.o: threadpool.c threadpool.h
	$(CC) $(CFLAGS) -c threadpool.c -o threadpool.o

mapreduce.o: mapreduce.c mapreduce.h
	$(CC) $(CFLAGS) -c mapreduce.c -o mapreduce.o

distwc.o: distwc.c mapreduce.h
	$(CC) $(CFLAGS) -c distwc.c -o distwc.o

# Link object files into the final executable
wordcount: threadpool.o mapreduce.o distwc.o
	$(CC) -o wordcount threadpool.o mapreduce.o distwc.o $(CFLAGS) -lpthread

# Clean up build files
clean:
	rm -f *.o wordcount
