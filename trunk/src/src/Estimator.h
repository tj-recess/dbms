#ifndef ESTIMATOR_H_
#define ESTIMATOR_H_

#include <map>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include "ParseTree.h"
#include "EventLogger.h"
using namespace std;


#define _ESTIMATOR_DEBUG 1

class Estimator
{
private:
	// members
	struct FuncOperator * m_pFuncOp;
	struct TableList * m_pTblList;
	struct AndList * m_pCNF;

	int m_nNumTables;
	vector <string> m_vSortedTables;
	char ** m_aTableNames;
	vector <string> m_vWholeCNF;	// break the AndList into tokens
	map <string, long long unsigned int> m_mJoinEstimate;

	// functions
	Estimator();
	int SortTables();
	void TokenizeAndList();
	void PrintTokenizedAndList();	// TODO: delete this
	void PopulateTableNames();
	void TableComboBaseCase(vector <string> &);
	int ComboAfterCurrTable(vector<string> &, string);
	void PrintFuncOpRecur(struct FuncOperator *func_node);

public:
	Estimator(struct FuncOperator *finalFunction,
			  struct TableList *tables,
			  struct AndList * boolean);
	~Estimator();

	void PrintFuncOperator();
	void PrintTableList();
	vector<string> PrintTableCombinations(int combo_len);
	
};

#endif
