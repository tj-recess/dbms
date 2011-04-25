#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "Function.h"
#include <string>
#include <iostream>

using namespace std;

class QueryPlanNode
{
public:
	// common members
	int m_nInPipe, m_nOutPipe;
	string m_sInFileName, m_sOutFileName;

	// left and right children (tree structure)
	QueryPlanNode * left;
	QueryPlanNode * right;
	QueryPlanNode * parent;

	QueryPlanNode() : m_nInPipe(-1), m_nOutPipe(-1), m_sInFileName(), m_sOutFileName(),
					left(NULL), right(NULL), parent(NULL)
	{}

	virtual void PrintNode()=0;
	virtual ~QueryPlanNode();
};

class Node_SelectPipe : public QueryPlanNode
{
public:
	CNF* m_pCNF;
	Record * m_pLiteral;
	
	Node_SelectPipe(int in, int out, CNF* pCNF, Record * pLit) 
	{
		m_nInPipe = in;
		m_nOutPipe = out;
		m_pCNF = pCNF;
		m_pLiteral = pLit;
	}
 
	void PrintNode()
	{
		cout << "\n*** Select Pipe Operation ***";
		cout << "\nInput pipe ID: " << m_nInPipe;
		cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nSelect CNF : ";
		m_pCNF->Print();
//		cout << "\nRecord Literal: " << m_pLiteral->print(); 
	}
};

class Node_SelectFile : public QueryPlanNode
{
public:
	CNF* m_pCNF;
    Record * m_pLiteral;

	Node_SelectFile(string inFile, int out, CNF* pCNF, Record * pLit) 
    {
		m_sInFileName = inFile;
		m_nOutPipe = out;
		m_pCNF = pCNF;
		m_pLiteral = pLit;
	}

    void PrintNode()
    {
        cout << "\n*** Select File Operation ***";
        cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nInput filename: " << m_sInFileName.c_str();
        cout << "\nSelect CNF : ";
		m_pCNF->Print();
//        cout << "\nRecord Literal: " << m_pLiteral->print();
    }
};

class Node_Project : public QueryPlanNode
{
public:
	int * atts_list;
	int m_nAttsToKeep, m_nTotalAtts;

	Node_Project(int ip, int op, int *atk, int nKeep, int nTot)
	{
		m_nInPipe = ip;
		m_nOutPipe = op;
		atts_list = atk;
		m_nAttsToKeep = nKeep;
		m_nTotalAtts = nTot;
	}
		
    void PrintNode()
    {
        cout << "\n*** Select Pipe Operation ***";
        cout << "\nInput pipe ID: " << m_nInPipe;
        cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nNum atts to Keep: " << m_nAttsToKeep;
		cout << "\nNum total atts: " << m_nTotalAtts;
    }
};

class Node_Join : public QueryPlanNode
{
public:
	int m_nRightInPipe;
    CNF* m_pCNF;
    Record * m_pLiteral;

    Node_Join(int ip1, int ip2, int op, CNF* pCNF, Record * pLit)
    {
		m_nInPipe = ip1;
		m_nRightInPipe = ip2;
		m_nOutPipe = op;
		m_pCNF = pCNF;
		m_pLiteral = pLit;
	}

    void PrintNode()
    {
        cout << "\n*** Join Operation ***";
        cout << "\nInput pipe-1 ID: " << m_nInPipe;
        cout << "\nInput pipe-2 ID: " << m_nRightInPipe;
        cout << "\nOutput pipe ID: " << m_nOutPipe;
        cout << "\nSelect CNF : ";
		m_pCNF->Print();
//        cout << "\nRecord Literal: " << m_pLiteral->print();
    }
};

class Node_Sum : public QueryPlanNode
{
public:
	Function * m_pFunc;

	Node_Sum(int ip, int op, Function *pF)
	{
		m_nInPipe = ip;
		m_nOutPipe = op;
		m_pFunc = pF;
	}

	void PrintNode()
	{
		cout << "\n*** Sum Operation ***";
        cout << "\nInput pipe ID: " << m_nInPipe;
        cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nFunction: ";
		m_pFunc->Print();
	}
};

class Node_GroupBy : public QueryPlanNode
{
public:
	Function * m_pFunc;
	OrderMaker * m_pOM;

	Node_GroupBy(int ip, int op, Function *pF, OrderMaker *pOM)
	{
		m_nInPipe = ip;
		m_nOutPipe = op;
		m_pFunc = pF;
		m_pOM = pOM;
	}

	void PrintNode()
	{
		cout << "\n*** Group-by Operation ***";
        cout << "\nInput pipe ID: " << m_nInPipe;
        cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nFunction: ";
		m_pFunc->Print();
		cout << "\nOrderMaker: ";
		m_pOM->Print();
    }
};

class Node_WriteOut : public QueryPlanNode
{
public:
	Schema * m_pSchema;

    Node_WriteOut(int ip, string outFile, Schema * pSch)
    {
		m_nInPipe = ip;
		m_sOutFileName = outFile;
		m_pSchema = pSch;
	}

    void PrintNode()
    {
        cout << "\n*** WriteOut Operation ***";
        cout << "\nInput pipe ID: " << m_nInPipe;
        cout << "\nOutput file: " << m_sOutFileName;
//        cout << "\nSchema: " << m_pSchema->Print();
    }
};

#endif
