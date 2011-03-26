all: a3.out a2-2test.out a2test.out a1test.out

CC = g++ -O0 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

a3.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o EventLogger.o FileUtil.o Heap.o Sorted.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3test.o
	$(CC) -o a3.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o EventLogger.o FileUtil.o Heap.o Sorted.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3test.o -lfl -lpthread

a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o FileUtil.o Heap.o Sorted.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a3test.o EventLogger.o a2-2test.o
	$(CC) -o a2-2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o EventLogger.o FileUtil.o Heap.o Sorted.o -lfl -lpthread

a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o FileUtil.o Heap.o Sorted.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o EventLogger.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o EventLogger.o FileUtil.o Heap.o Sorted.o -lfl -lpthread

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o FileUtil.o Heap.o Sorted.o BigQ.o DBFile.o Pipe.o EventLogger.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o FileUtil.o Heap.o Sorted.o BigQ.o EventLogger.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl -lpthread

a3test.o: a3test.cc
	$(CC) -g -c a3test.cc

a2-2test.o: a2-2test.cc
	$(CC) -g -c a2-2test.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sorted.o: Sorted.cc
	$(CC) -g -c Sorted.cc

FileUtil.o: FileUtil.cc
	$(CC) -g -c FileUtil.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

EventLogger.o: EventLogger.cc
	$(CC) -g -c EventLogger.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
                
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c
        
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
