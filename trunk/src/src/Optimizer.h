#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include <map>
#include <utility>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include "ParseTree.h"
#include "EventLogger.h"
#include "Statistics.h"
#include "QueryPlan.h"
#include "Record.h"
#include "Schema.h"

using namespace std;


#define _DEBUG_OPTIMIZER 1

class Optimizer
{
private:
	// members
	struct FuncOperator * m_pFuncOp;
	struct TableList * m_pTblList;
	struct AndList * m_pCNF;
	struct NameList * m_pGroupingAtts; // grouping atts (NULL if no grouping)
	struct NameList * m_pAttsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
	int m_nDistinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
	int m_nDistinctFunc; 

	int m_nNumTables, m_nGlobalPipeID;
	//vector <string> m_vSortedTables;
	vector <string> m_vSortedAlias;
	vector <string> m_vTabCombos;
        QueryPlanNode *m_pFinalJoin;

	char ** m_aTableNames;
	vector <string> m_vWholeCNF;	// break the AndList into tokens
        struct JoinValue
        {
            Statistics *stats;
            QueryPlanNode *queryPlanNode;
            string joinOrder;
//            Schema schema;
        };
//	map <string, pair <Statistics *, QueryPlanNode*> > m_mJoinEstimate;
    map <string, JoinValue> m_mJoinEstimate;
	Statistics m_Stats;								// master copy of the stats object
    map <string, string> m_mAliasToTable;           // alias to original table name
	map <int, string> m_mOutPipeToCombo;			// map of outpipe to combo name
        map <string, AndList*> m_mAliasToAndList;   //map of alias and their corresponding AndList
        map <AndList*, bool> m_mAndListToUsage; //map of AndList* to track if they have been used already or not

	// functions
	Optimizer();
	//int SortTables();
	int SortAlias();
	void TokenizeAndList(AndList*);
	void PopulateTableNames(vector<string> & vec_rels);		// in m_aTableNames char** array
	void ComboToVector(string, vector <string> & vec_rels); // break A.B.C into vector(A,B,C)
	void TableComboBaseCase(vector <string> &);
	int ComboAfterCurrTable(vector<string> &, string);
	void PrintFuncOpRecur(struct FuncOperator *func_node);
	AndList* GetSelectionsFromAndList(string aTableName);
	AndList* GetJoinsFromAndList(vector<string>&);
    void RemoveAliasFromColumnName(AndList* parseTreeNode);
	void ConcatSchemas(Schema *pRSch, Schema *pLSch, string sName);

	// Functions for debugging
	void PrintTokenizedAndList();	// TODO: delete this
	void print_map();

    void PrintOrList(struct OrList *pOr);
    void PrintComparisonOp(struct ComparisonOp *pCom);
    void PrintOperand(struct Operand *pOperand);
    void PrintAndList(struct AndList *pAnd);
    //pair<string, string>* FindOptimalPairing(vector<string>& vAliases,  AndList* parseTree);
    void FindOptimalPairing(vector<string>& vAliases,  AndList* parseTree, pair<string, string> &);

    void PrintAndListToUsageMap();
    void PrintAliasToAndListMap();

public:
	Optimizer(struct FuncOperator *finalFunction,
			  struct TableList *tables,
			  struct AndList * boolean,
			  struct NameList * pGrpAtts,
              struct NameList * pAttsToSelect,
              int distinct_atts, int distinct_func,
			  Statistics & s);
	~Optimizer();

	void PrintFuncOperator();
	void PrintTableList();
	void MakeQueryPlan();
        void ExecuteQuery();
	vector<string> PrintTableCombinations(int combo_len);
	
};

//

#endif
