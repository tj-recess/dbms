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

#define QUERY_PIPE_SIZE 1000

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
	QueryPlanNode * parent;

	QueryPlanNode() : m_nInPipe(-1), m_nOutPipe(-1), m_sInFileName(), m_sOutFileName(),
					left(NULL), right(NULL), parent(NULL)
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
	}
 
	void PrintNode()
	{
        if (this->left != NULL)
            this->left->PrintNode();

		cout << "\n*** Select Pipe Operation ***";
		cout << "\nInput pipe ID: " << m_nInPipe;
		cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nSelect CNF : ";
		if (m_pCNF != NULL)
			m_pCNF->Print();
		else
			cout << "NULL";
//		cout << "\nRecord Literal: " << m_pLiteral->print(); 
		cout << endl << endl;

        if (this->right != NULL)
            this->right->PrintNode();
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

    void ExecutePostOrder()
    {
        if (this->left)
            this->left->ExecutePostOrder();
        if (this->right)
            this->right->ExecutePostOrder();
        this->ExecuteNode();
    }

    void ExecuteNode()
    {
		cout << "\nExecuteNode of SelectPipe\n";
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
        m_mPipes[out] = new Pipe(QUERY_PIPE_SIZE);
	}

    void PrintNode()
    {
        if (this->left != NULL)
            this->left->PrintNode();

        cout << "\n*** Select File Operation ***";
        cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nInput filename: " << m_sInFileName.c_str();
        cout << "\nSelect CNF : ";
        if (m_pCNF != NULL)
            m_pCNF->Print();
        else
            cout << "NULL";
//        cout << "\nRecord Literal: " << m_pLiteral->print();
		cout << endl << endl;
        if (this->right != NULL)
            this->right->PrintNode();
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

    void ExecutePostOrder()
    {
		if (this->left)
	        this->left->ExecutePostOrder();
		if (this->right)
	        this->right->ExecutePostOrder();
        this->ExecuteNode();
    }

    void ExecuteNode()
    {
        //create a DBFile from input file path provided
        DBFile inFile;
        inFile.Open(const_cast<char*>(m_sInFileName.c_str()));
		
		cout << "\n IN selectFile for " << m_sInFileName.c_str() << endl;
        SelectFile sf;
		if (m_pCNF != NULL && m_pLiteral != NULL)
		{
	        sf.Run(inFile, *(m_mPipes[m_nOutPipe]), *m_pCNF, *m_pLiteral);


			int dotPos = m_sInFileName.find(".");
			string sTabName = m_sInFileName.substr(0, dotPos);
			Schema Sch("catalog", (char*)sTabName.c_str());
			Record rec;
			int count = 0;
			while (m_mPipes[m_nOutPipe]->Remove (&rec)) 
			{
            	//rec.Print(&Sch);
				count++;
		    }
			cout << endl << count << " records removed from pipe " << m_nOutPipe << endl;
		}
		else
			cout << "\nInsufficient parameters!\n";
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
        if (this->left != NULL)
            this->left->PrintNode();

        cout << "\n*** Select Pipe Operation ***";
        cout << "\nInput pipe ID: " << m_nInPipe;
        cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nNum atts to Keep: " << m_nAttsToKeep;
		cout << "\nNum total atts: " << m_nTotalAtts;
		if (atts_list != NULL)
		{
			cout << "\nAttributes to keep: ";
			for (int i = 0; i < m_nAttsToKeep; i++)
				cout << atts_list[i] << "  ";
		}
		cout << endl << endl;
        if (this->right != NULL)
            this->right->PrintNode();
    }
	
	~Node_Project()
	{
		delete [] atts_list; atts_list = NULL;
	}

    void ExecutePostOrder()
    {
        if (this->left)
            this->left->ExecutePostOrder();
        if (this->right)
            this->right->ExecutePostOrder();
        this->ExecuteNode();
    }

    void ExecuteNode()
    {
        cout << "\nExecuteNode of Node_Project\n";
    }
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
	}

        Node_Join(int ip1, int ip2, int op, Schema * pSch, AndList* parseTree)
        {
		m_nInPipe = ip1;
		m_nRightInPipe = ip2;
		m_nOutPipe = op;
		//m_pCNF = pCNF;
		m_pSchema = pSch;
//		m_pLiteral = pLit;
                
	}

    void PrintNode()
    {
		if (this->left != NULL)
			this->left->PrintNode();
        cout << "\n*** Join Operation ***";
        cout << "\nInput pipe-1 ID: " << m_nInPipe;
        cout << "\nInput pipe-2 ID: " << m_nRightInPipe;
        cout << "\nOutput pipe ID: " << m_nOutPipe;
        cout << "\nSelect CNF : ";
        if (m_pCNF != NULL)
            m_pCNF->Print();
        else
            cout << "NULL";
//        cout << "\nRecord Literal: " << m_pLiteral->print();
		cout << endl << endl;
		if (this->right != NULL)
			this->right->PrintNode();
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

    void ExecutePostOrder()
    {
        if (this->left)
            this->left->ExecutePostOrder();
        if (this->right)
            this->right->ExecutePostOrder();
        this->ExecuteNode();
    }

    void ExecuteNode()
    {
        cout << "\nExecuteNode of Node_Join\n";
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
        if (this->left != NULL)
            this->left->PrintNode();

		cout << "\n*** Sum Operation ***";
        cout << "\nInput pipe ID: " << m_nInPipe;
        cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nFunction: ";
		m_pFunc->Print();
		cout << endl << endl;

        if (this->right != NULL)
            this->right->PrintNode();
	}

	~Node_Sum()
	{
		delete m_pFunc; m_pFunc = NULL;
	}

    void ExecutePostOrder()
    {
        if (this->left)
            this->left->ExecutePostOrder();
        if (this->right)
            this->right->ExecutePostOrder();
        this->ExecuteNode();
    }

    void ExecuteNode()
    {
        cout << "\nExecuteNode of Node_Sum\n";
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
        if (this->left != NULL)
            this->left->PrintNode();

		cout << "\n*** Group-by Operation ***";
        cout << "\nInput pipe ID: " << m_nInPipe;
        cout << "\nOutput pipe ID: " << m_nOutPipe;
		cout << "\nFunction: ";
		if (m_pFunc)
			m_pFunc->Print();
		else
			cout << "NULL\n";
		cout << "\nOrderMaker:\n";
		if (m_pOM)
			m_pOM->Print();
        else
            cout << "NULL\n";
		
		cout << endl << endl;

        if (this->right != NULL)
            this->right->PrintNode();
    }

	~Node_GroupBy()
	{
		delete m_pFunc; m_pFunc = NULL;
		delete m_pOM; m_pOM = NULL;
	}

    void ExecutePostOrder()
    {
        if (this->left)
            this->left->ExecutePostOrder();
        if (this->right)
            this->right->ExecutePostOrder();
        this->ExecuteNode();
    }

    void ExecuteNode()
    {
        cout << "\nExecuteNode of Node_GroupBy\n";
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
        if (this->left != NULL)
            this->left->PrintNode();

        cout << "\n*** WriteOut Operation ***";
        cout << "\nInput pipe ID: " << m_nInPipe;
        cout << "\nOutput file: " << m_sOutFileName;
//        cout << "\nSchema: " << m_pSchema->Print();
		cout << endl << endl;

        if (this->right != NULL)
            this->right->PrintNode();
    }

	~Node_WriteOut()
	{
		delete m_pSchema; m_pSchema = NULL;
	}

    void ExecutePostOrder()
    {
        if (this->left)
            this->left->ExecutePostOrder();
        if (this->right)
            this->right->ExecutePostOrder();
        this->ExecuteNode();
    }

    void ExecuteNode()
    {
        cout << "\nExecuteNode of Node_WriteOut\n";
    }
};

#endif
