
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "Optimizer.h"
#include "DDL_DML.h"

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

extern struct NameList *sortingAtts;   // sort the file on these attributes (NULL if no grouping atts)
extern struct NameList *table_name;    // create table name
extern struct NameList *file_name;     // input file to load into table
extern struct AttsList *col_atts;
extern int selectFromTable;// 1 if the SQL is select from table
extern int createTable;    // 1 if the SQL is create table
extern int sortedTable;    // 0 = create table as heap, 1 = create table as sorted (use sortingAtts)
extern int insertTable;    // 1 if the command is Insert into table
extern int dropTable;      // 1 is the command is Drop table

int main () 
{
    yyparse();

	// --------- SELECT query -------------
	if (selectFromTable == 1)
	{
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

	// ----------- project 5 ----------
	Attribute try_atts[3];
	string sTabName = "mal_test";
	string sTabType = "HEAP";
	char *col_names[3] = {"mal_col1","mal_col2","mal_col3"};
	try_atts[0].name = col_names[0];	try_atts[0].myType = Int;
	try_atts[1].name = col_names[1];	try_atts[1].myType = String;
	try_atts[2].name = col_names[2];	try_atts[2].myType = Double;
	
	DDL_DML ddObj;
	ddObj.CreateTable(sTabName, try_atts, 3, sTabType);

	// --------- CREATE TABLE query -------------
	if (createTable == 1)
	{
		cout << "\n Create table statement\n";
		if (sortedTable == 1)
		{
			cout << "\n Create table as sorted\n";
			if (sortingAtts == NULL)
				cout << "\n But sorting atts are NULL!!\n";
			else
				cout << "\n Sorting atts exist!\n";
		}
		else
			cout << "\n Create table as heap\n";
	}
    // --------- INSERT INTO TABLE query -------------
	else if (insertTable == 1)
	{
		cout << "\n Insert into table command\n";
		if (table_name != NULL && file_name != NULL)
			cout << "\nTable and File names exist!\n";
		else
			cout << "\nEither table or file names is NULL!\n";
	}
    // --------- DROP TABLE query -------------
	else if (dropTable == 1)
    {
        cout << "\n Drop table command\n";
        if (table_name != NULL)
            cout << "\nTable name exists!\n";
        else
            cout << "\nTable name is NULL!\n";
    }
}


