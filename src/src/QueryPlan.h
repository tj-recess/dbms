#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "Function.h"
#include "DBFile.h"
#include "RelOp.h"
#include <string>
#include <iostream>
#include <map>

#define QUERY_PIPE_SIZE 1000
#define QUERY_USE_PAGES 100

using namespace std;

class QueryPlanNode
{
public:
	// common members
	int m_nInPipe, m_nOutPipe;
	string m_sInFileName, m_sOutFileName;
    map<int, Pipe*> m_mPipes;

	// left and right children (tree structure)
	QueryPlanNode * left;
	QueryPlanNode * right;

	QueryPlanNode() : m_nInPipe(-1), m_nOutPipe(-1), m_sInFileName(), m_sOutFileName(),
					left(NULL), right(NULL)
	{}

	virtual void PrintNode() {}
    virtual void ExecutePostOrder() {}
    virtual void ExecuteNode() {}
	virtual ~QueryPlanNode() {}	
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
		m_mPipes[m_nOutPipe] = new Pipe(QUERY_PIPE_SIZE);
	}
 
	~Node_SelectPipe()
    {
		if (m_pCNF)
		{
        	delete m_pCNF; m_pCNF = NULL;
		}
		if (m_pLiteral)
		{
        	delete m_pLiteral; m_pLiteral = NULL;
		}
    }

	void PrintNode();
    void ExecutePostOrder();
    void ExecuteNode();

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
        m_mPipes[m_nOutPipe] = new Pipe(QUERY_PIPE_SIZE);
	}

	~Node_SelectFile()
	{
        if (m_pCNF)
        {
            delete m_pCNF; m_pCNF = NULL;
        }
        if (m_pLiteral)
        {
            delete m_pLiteral; m_pLiteral = NULL;
        }
	}

    void PrintNode();
    void ExecutePostOrder();
    void ExecuteNode();
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
		m_mPipes[m_nOutPipe] = new Pipe(QUERY_PIPE_SIZE);
	}
		
	~Node_Project()
	{
		if (atts_list)
		{
			delete [] atts_list; 
			atts_list = NULL;
		}
	}

    void PrintNode();
    void ExecutePostOrder();
    void ExecuteNode();
};

class Node_Join : public QueryPlanNode
{
public:
	int m_nRightInPipe;
    CNF* m_pCNF;
    Record * m_pLiteral;
	Schema * m_pSchema;

    Node_Join(int ip1, int ip2, int op, CNF* pCNF, Schema * pSch, Record * pLit)
    {
		m_nInPipe = ip1;
		m_nRightInPipe = ip2;
		m_nOutPipe = op;
		m_pCNF = pCNF;
		m_pSchema = pSch;
		m_pLiteral = pLit;
		m_mPipes[m_nOutPipe] = new Pipe(QUERY_PIPE_SIZE);		
	}
	
	~Node_Join()
    {
        if (m_pCNF)
        {
            delete m_pCNF; m_pCNF = NULL;
        }
        if (m_pLiteral)
        {
            delete m_pLiteral; m_pLiteral = NULL;
        }
		if (m_pSchema)
		{
    	    delete m_pSchema; m_pSchema = NULL;
		}
    }

    void PrintNode();
    void ExecutePostOrder();
    void ExecuteNode();
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
		m_mPipes[m_nOutPipe] = new Pipe(QUERY_PIPE_SIZE);
	}

	~Node_Sum()
	{
		if (m_pFunc)
		{
			delete m_pFunc; m_pFunc = NULL;
		}
	}

	void PrintNode();
    void ExecutePostOrder();
    void ExecuteNode();
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
		m_mPipes[m_nOutPipe] = new Pipe(QUERY_PIPE_SIZE);
	}

	~Node_GroupBy()
	{
		if (m_pFunc)
		{
			delete m_pFunc; m_pFunc = NULL;
		}
		if (m_pOM)
		{
			delete m_pOM; m_pOM = NULL;
		}
	}

    void PrintNode();
    void ExecutePostOrder();
    void ExecuteNode();
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

	~Node_WriteOut()
	{
		if (m_pSchema)
		{
			delete m_pSchema; m_pSchema = NULL;
		}
	}

    void PrintNode();
    void ExecutePostOrder();
    void ExecuteNode();
};

#endif
