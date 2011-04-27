
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
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

int main () 
{
    yyparse();

    char *fileName = "Statistics.txt";
    Statistics::PrepareStatisticsFile(fileName);
    Statistics s;
    s.Read(fileName);

	// Start estimator after Stats object is ready
	Optimizer Oz(finalFunction, tables, boolean, groupingAtts, 
				 attsToSelect, distinctAtts, distinctFunc, s);
	//Oz.PrintFuncOperator();
	//Oz.PrintTableList();
	Oz.MakeQueryPlan();

        Oz.ExecuteQuery();

}


