
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "Estimator.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;

int main () 
{
    yyparse();

	Estimator E(finalFunction, tables);

	E.PrintFuncOperator();
	E.PrintTableList();
	
    char *fileName = "Statistics.txt";
    Statistics::PrepareStatisticsFile(fileName);
    Statistics s;
    s.Read(fileName);
}


