#ifndef ESTIMATOR_H_
#define ESTIMATOR_H_

#include <map>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include "ParseTree.h"
#include "EventLogger.h"
#include "Statistics.h"

using namespace std;


#define _ESTIMATOR_DEBUG 1

class Optimizer
{
private:
	// members
	struct FuncOperator * m_pFuncOp;
	struct TableList * m_pTblList;
	struct AndList * m_pCNF;

	int m_nNumTables;
	vector <string> m_vSortedTables;
	vector <string> m_vSortedAlias;
	char ** m_aTableNames;
	vector <string> m_vWholeCNF;	// break the AndList into tokens
	map <string, long long unsigned int> m_mJoinEstimate;
	Statistics m_Stats;

	// functions
	Optimizer();
	int SortTables();
	int SortAlias();
	void TokenizeAndList();
	void PrintTokenizedAndList();	// TODO: delete this
	void PopulateTableNames();		// in m_aTableNames char** array
	void TableComboBaseCase(vector <string> &);
	int ComboAfterCurrTable(vector<string> &, string);
	void PrintFuncOpRecur(struct FuncOperator *func_node);


public:
	Optimizer(struct FuncOperator *finalFunction,
			  struct TableList *tables,
			  struct AndList * boolean,
			  Statistics & s);
	~Optimizer();

	void PrintFuncOperator();
	void PrintTableList();
	vector<string> PrintTableCombinations(int combo_len);
	
};

#endif
