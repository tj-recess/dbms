
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "Optimizer.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;

int main () 
{
    yyparse();

    char *fileName = "Statistics.txt";
    Statistics::PrepareStatisticsFile(fileName);
    Statistics s;
    s.Read(fileName);

	// Start estimator after Stats object is ready
	Optimizer Oz(finalFunction, tables, boolean, s);
	//Oz.PrintFuncOperator();
	//Oz.PrintTableList();

}


