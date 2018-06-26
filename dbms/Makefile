src = $(wildcard *.c)
exec = main.out
obj = $(src:.c=.o)

TEMP_FLAGS = -Wno-implicit-function-declaration
CFLAGS += -Wall -g -std=c99 -I ../cdata-ddl-compiler $(TEMP_FLAGS)


.PHONY: all a.out
all:comp
comp:$(obj)

$(exec):$(obj)
	gcc $(CFLAGS) $^
	./$(exe)

clean:
	$(RM) $(obj) $(exe)
