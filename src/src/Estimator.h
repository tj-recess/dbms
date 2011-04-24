#ifndef ESTIMATOR_H_
#define ESTIMATOR_H_

#include <map>
#include <vector>
#include <string>
#include <iostream>
#include "ParseTree.h"
using namespace std;

//extern struct FuncOperator *finalFunction;
//extern struct TableList *tables;

class Estimator
{
private:
	// members
	struct FuncOperator * m_pFuncOp;
	struct TableList * m_pTblList;
	int m_nNumTables;
	vector <string> m_vSortedTables;
	map <string, long long unsigned int> m_mJoinEstimate;

	// functions
	Estimator();
	int SortTables();
	void TableComboBaseCase(vector <string> &);
	int ComboAfterCurrTable(vector<string> &, string);
	void PrintFuncOpRecur(struct FuncOperator *func_node);

public:
	Estimator(struct FuncOperator *finalFunction,
			  struct TableList *tables);
	~Estimator();

	void PrintFuncOperator();
	void PrintTableList();
	vector<string> PrintTableCombinations(int combo_len);
	
};

#endif
