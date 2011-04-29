/*

Name: main.cc
Purpose: The interface for the database

*/

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
extern struct NameList *groupingAtts; 	// grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; 	// the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; 				// 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  				// 1 if there is a DISTINCT in an aggregate query

extern struct NameList *sortingAtts;   	// sort the file on these attributes (NULL if no grouping atts)
extern struct NameList *table_name;    	// create table name
extern struct NameList *file_name;     	// input file to load into table
extern struct AttsList *col_atts;		// Column name and type in create table
extern int selectFromTable;				// 1 if the SQL is select from table
extern int createTable;    				// 1 if the SQL is create table
extern int sortedTable;    				// 0 = create table as heap, 1 = create table as sorted (use sortingAtts)
extern int insertTable;    				// 1 if the command is Insert into table
extern int dropTable;      				// 1 is the command is Drop table
extern int printPlanOnScreen;  			// 1 if true
extern int executePlan;        			// 1 if true
extern struct NameList *outputFileName; // Name of the file where plan should be printed

struct CurrSession
{
	int nOnScreen, nExecute;
	string sFileName;
};

void ReadSession(struct CurrSession & cs);

int main () 
{
	CurrSession cs;
	ReadSession(cs);

	cout << "\n\n----------------------------------------------------------\n"
		 << "The database is ready. Please enter a valid SQL statement: \n\n";

	// Get SQL query from the user
    yyparse();

	// --------- SELECT query -------------
	if (selectFromTable == 1)
	{
	    char *fileName = "Statistics.txt";
    	Statistics::PrepareStatisticsFile(fileName);
	    Statistics StatsObj;
	    StatsObj.Read(fileName);

		// Start estimator after Stats object is ready
		// And pass all the relevant attributes to the optimizer
		Optimizer Oz(StatsObj, finalFunction, tables, boolean, groupingAtts, 
					 	attsToSelect, distinctAtts, distinctFunc, 
						cs.nOnScreen, cs.sFileName);
						
		//Oz.PrintFuncOperator();
		//Oz.PrintTableList();
		Oz.MakeQueryPlan();
	
		// Execute the query according to the plan only if asked
		if (cs.nExecute == 1)
	    	Oz.ExecuteQuery();
	
		return 0;
	}

	// --------- CREATE TABLE query -------------
	else if (createTable == 1)
	{
		DDL_DML ddObj;
		cout << "\nExecuting... Create table statement\n";
		vector <Attribute> ColAttsVec;
		string sTableName;

		// Fetch table name to create
		if (table_name != NULL)
			sTableName = table_name->name;
		else
		{
			cerr << "\nERROR! No table name specified!\n";
			return 1;
		}

		// Fetch column attributes
		AttsList * temp = col_atts;
		while (temp != NULL)
		{
			Attribute Atts;
			Atts.name = temp->name;
			if (temp->code == 1)
				Atts.myType = Double;
			else if (temp->code == 2)
				Atts.myType = Int;
			else if (temp->code == 4)
				Atts.myType = String;
		
			ColAttsVec.push_back(Atts);

			temp = temp->next;
		}

		// SORTED table
		if (sortedTable == 1)
		{
			cout << "\tCreate table as sorted\n";
			if (sortingAtts == NULL)
			{
				cerr << "\nERROR! Sorted table needs columns on which it is sorted!\n";
				return 1;
			}
			else
			{
				vector <string> sort_cols_vec;
				NameList * temp = sortingAtts;
				while (temp != NULL)
				{
					sort_cols_vec.push_back(temp->name);
					temp = temp->next;
				}
				int ret = ddObj.CreateTable(sTableName, ColAttsVec, true, &sort_cols_vec);
				if (ret == RET_TABLE_ALREADY_EXISTS)
					cerr << "Table " << sTableName.c_str() << " already exists in the database!\n";
				else if (ret == RET_CREATE_TABLE_SORTED_COLS_DONOT_MATCH)
					cerr << "\nERROR! Sorted column doesn't match table column\n";
				
			}
		}
		else	// HEAP table
		{
			int ret = ddObj.CreateTable(sTableName, ColAttsVec);
			if (ret == RET_TABLE_ALREADY_EXISTS)
                    cerr << "Table " << sTableName.c_str() << " already exists in the database!\n";
		}
	}

    // --------- INSERT INTO TABLE query -------------
	else if (insertTable == 1)
	{
		DDL_DML ddObj;
		cout << "\nExecuting... Insert into table command\n";
		if (table_name== NULL || file_name == NULL)
		{
			cerr << "\nERROR! No table-name or file-name specified to load!\n";
			return 1;
		}
		else
		{
			string sTableName = table_name->name;
			string sFileName = file_name->name;
			int ret = ddObj.LoadTable(sTableName, sFileName);
			if (ret == RET_COULDNT_OPEN_FILE_TO_LOAD)
				cerr << "\nERROR! Could not locate metadata for table " << sTableName.c_str()
					 << endl;
		}
	}

    // --------- DROP TABLE query -------------
	else if (dropTable == 1)
    {
		DDL_DML ddObj;
        cout << "\nExecuting... Drop table command\n";
        if (table_name == NULL)
            cout << "\nTable name is NULL!\n";
		else
		{
			string sTableName = table_name->name;
			int ret = ddObj.DropTable(sTableName);
			if (ret == RET_COULDNT_OPEN_CATALOG_FILE)
				cerr << "\nCouldn't open catalog file for reading\n";
			else if (ret == RET_TABLE_NOT_IN_DATABASE)
				cerr << "\nTable " << sTableName.c_str() << " not found in the database!\n";
		}
    }
	
	// ----------- session variable --------------
	else
	{
		if (printPlanOnScreen == 0 && executePlan == 0 && outputFileName == NULL)
			return 0;		// Can't have all this, probably syntax error?

		ofstream session;
		session.open("Session.conf");
		session << printPlanOnScreen << endl;
		session << executePlan << endl;
		if (outputFileName != NULL)
			session << outputFileName->name << endl;
		else
			session << "NONE" << endl;
		session.close();
		cout << "\nNew session variables have been stored.\n";
	}

	return 0;
}

void ReadSession(struct CurrSession & cs)
{
	ifstream session;
	session.open("Session.conf");
	if (!session)
		cerr << "\nERROR! Session.conf not found!\n\n";
	else
	{
		int print_on_screen, execute_plan;
		string file_name;
		session >> print_on_screen;
		session >> execute_plan;
		session >> file_name;
		//getline(session, file_name);
		cout << "\n\n-------------------------\n"
			 << "Current Session settings:\n"
			 << "-------------------------\n";

		if (print_on_screen == 1)
			cout << "Print query plan on screen : TRUE\n";
		else
			cout << "Print query plan on screen : FALSE\n";
	
		if (execute_plan == 1)
			cout << "Execute query plan : TRUE\n";
        else
            cout << "Execute query plan : FALSE\n";

		cout << "Write query plan in file: " << file_name.c_str() << endl;

		cs.nOnScreen = print_on_screen;
		cs.nExecute = execute_plan;
		cs.sFileName = file_name;

		session.close();
	}
}
