#include "QueryPlan.h"

using namespace std;

// Initialize static map
map<int, Pipe*> QueryPlanNode::m_mPipes;

// -------------------------------------- select pipe ------------------
void Node_SelectPipe::PrintNode()
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
	cout << endl << endl;

	if (this->right != NULL)
		this->right->PrintNode();
}

void Node_SelectPipe::ExecutePostOrder()
{
	if (this->left)
    	this->left->ExecutePostOrder();
    if (this->right)
        this->right->ExecutePostOrder();
    this->ExecuteNode();
}

void Node_SelectPipe::ExecuteNode()
{
	cout << "\nExecuteNode of SelectPipe\n";
}

// -------------------------------------- select file ------------------

void Node_SelectFile::PrintNode()
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
    cout << endl << endl;
    if (this->right != NULL)
        this->right->PrintNode();
}

void Node_SelectFile::ExecutePostOrder()
{
    if (this->left)
        this->left->ExecutePostOrder();
    if (this->right)
        this->right->ExecutePostOrder();
    this->ExecuteNode();
}

void Node_SelectFile::ExecuteNode()
{
        //create a DBFile from input file path provided
        DBFile * pFile = new DBFile;
        pFile->Open(const_cast<char*>(m_sInFileName.c_str()));

        cout << "\n IN selectFile for " << m_sInFileName.c_str() << endl;
        SelectFile * pSF = new SelectFile;
        pSF->Use_n_Pages(QUERY_USE_PAGES);
        if (m_pCNF != NULL && m_pLiteral != NULL)
        {
            pSF->Run(*pFile, *(QueryPlanNode::m_mPipes[m_nOutPipe]), *m_pCNF, *m_pLiteral);

            //sf.WaitUntilDone();
/*
            int dotPos = m_sInFileName.find(".");
            string sTabName = m_sInFileName.substr(0, dotPos);
            Schema Sch("catalog", (char*)sTabName.c_str());
            Record rec;
            int count = 0;
            while (QueryPlanNode::m_mPipes[m_nOutPipe]->Remove (&rec))
            {
                //rec.Print(&Sch);
                count++;
            }
            cout << endl << count << " records removed from pipe " << m_nOutPipe << endl;
*/
        }
        else
            cout << "\nInsufficient parameters!\n";
}

// -------------------------------------- project ------------------

void Node_Project::PrintNode()
{
        if (this->left != NULL)
            this->left->PrintNode();

        cout << "\n*** Project Node Operation ***";
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

void Node_Project::ExecutePostOrder()
{
    if (this->left)
        this->left->ExecutePostOrder();
    if (this->right)
        this->right->ExecutePostOrder();
    this->ExecuteNode();
}

void Node_Project::ExecuteNode()
{
        cout << "\nExecuteNode of Node_Project\n";
        Project P;
        P.Use_n_Pages(QUERY_USE_PAGES);
        if (atts_list != NULL)
        {
            P.Run(*(QueryPlanNode::m_mPipes[m_nInPipe]), *(QueryPlanNode::m_mPipes[m_nOutPipe]),
                  atts_list, m_nTotalAtts, m_nAttsToKeep);

			if (m_bPrintHere == true)
			{
	            Record rec;
    	        int count = 0;
        	    while (QueryPlanNode::m_mPipes[m_nOutPipe]->Remove(&rec))
            	{
					//cout << "\ntrying to fetch recs after project\n";
	                count++;
    	        }
        	    cout << endl << count << " records removed from pipe " << m_nOutPipe << endl;
			}
        }
        else
            cout << "\nInsufficient parameters!\n";
}

// -------------------------------------- join ------------------

void Node_Join::PrintNode()
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
        cout << endl << endl;
        if (this->right != NULL)
            this->right->PrintNode();
}

void Node_Join::ExecutePostOrder()
{
    if (this->left)
        this->left->ExecutePostOrder();
    if (this->right)
        this->right->ExecutePostOrder();
    this->ExecuteNode();
}

void Node_Join::ExecuteNode()
{
        cout << "\n IN Join Node with outpipe " << m_nOutPipe << endl;

        Join J; 
        J.Use_n_Pages(QUERY_USE_PAGES);
        if (m_pCNF != NULL && m_pLiteral != NULL)
        {
            J.Run(*(QueryPlanNode::m_mPipes[m_nInPipe]), *(QueryPlanNode::m_mPipes[m_nRightInPipe]), 
                   *(QueryPlanNode::m_mPipes[m_nOutPipe]), *m_pCNF, *m_pLiteral);
        }
        else
            cout << "\nInsufficient parameters!\n";
}

// -------------------------------------- group by  ------------------
void Node_GroupBy::PrintNode()
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


void Node_GroupBy::ExecutePostOrder()
{
    if (this->left)
        this->left->ExecutePostOrder();
    if (this->right)
        this->right->ExecutePostOrder();
    this->ExecuteNode();
}

void Node_GroupBy::ExecuteNode()
{
	cout << "\nExecuteNode of Node_GroupBy\n";
	GroupBy G;        
    G.Use_n_Pages(QUERY_USE_PAGES);
    if (m_pFunc != NULL && m_pOM != NULL)
    {
		G.Run(*(QueryPlanNode::m_mPipes[m_nInPipe]), *(QueryPlanNode::m_mPipes[m_nOutPipe]), *m_pOM, *m_pFunc);
		cout << "\nOut of group.run\n";
		Record rec;
		Attribute DA = {"double", Double};
		Schema sum_sch ("sum_sch", 1, &DA);
		while (QueryPlanNode::m_mPipes[m_nOutPipe]->Remove (&rec)) 
		{
			cout << "\nTrying to fetch sum\n";
            rec.Print (&sum_sch);
        }
    }
    else
    	cout << "\nInsufficient parameters!\n";
}

// -------------------------------------- sum ------------------

void Node_Sum::PrintNode()
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

void Node_Sum::ExecutePostOrder()
{
    if (this->left)
        this->left->ExecutePostOrder();
    if (this->right)
        this->right->ExecutePostOrder();
    this->ExecuteNode();
}

void Node_Sum::ExecuteNode()
{
	cout << "\nExecuteNode of Node_Sum\n";
    Sum S;
    S.Use_n_Pages(QUERY_USE_PAGES);
    if (m_pFunc != NULL)
    {
        S.Run(*(QueryPlanNode::m_mPipes[m_nInPipe]), *(QueryPlanNode::m_mPipes[m_nOutPipe]), *m_pFunc);
		if (m_pFunc->ReturnsInt() == 1)
		{
            Record rec;
            Attribute IA = {"integer", Int};
            Schema sum_sch ("sum_sch", 1, &IA);
            while (QueryPlanNode::m_mPipes[m_nOutPipe]->Remove (&rec))
            {
                rec.Print (&sum_sch);
            }
		}
		else
		{
	        Record rec;
    	    Attribute DA = {"double", Double};
        	Schema sum_sch ("sum_sch", 1, &DA);
	        while (QueryPlanNode::m_mPipes[m_nOutPipe]->Remove (&rec))
    	    {
            	rec.Print (&sum_sch);
	        }
		}
    }
    else
        cout << "\nInsufficient parameters!\n";
}

// -------------------------------------- Distinct ------------------
void Node_Distinct::PrintNode()
{
    if (this->left != NULL)
        this->left->PrintNode();

    cout << "\n*** Distinct Operation ***";
    cout << "\nInput pipe ID: " << m_nInPipe;
    cout << "\nOutput pipe ID: " << m_nOutPipe;
    cout << endl << endl;

    if (this->right != NULL)
        this->right->PrintNode();
}

void Node_Distinct::ExecutePostOrder()
{
    if (this->left)
        this->left->ExecutePostOrder();
    if (this->right)
        this->right->ExecutePostOrder();
    this->ExecuteNode();
}

void Node_Distinct::ExecuteNode()
{
    cout << "\nExecuteNode of Distinct\n";
    DuplicateRemoval DR;
    DR.Use_n_Pages(QUERY_USE_PAGES);
    if (m_pSchema != NULL)
    {
        DR.Run(*(QueryPlanNode::m_mPipes[m_nInPipe]), *(QueryPlanNode::m_mPipes[m_nOutPipe]), *m_pSchema);

		// Clear the pipe here
        Record rec;
        int count = 0;
        while (QueryPlanNode::m_mPipes[m_nOutPipe]->Remove(&rec))
        {
    	    //cout << "\ntrying to fetch recs after project\n";
	         count++;
        }
        cout << endl << count << " records removed from pipe " << m_nOutPipe << endl;
	}	
}

// -------------------------------------- write out ------------------

void Node_WriteOut::PrintNode()
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

void Node_WriteOut::ExecutePostOrder()
{
    if (this->left)
        this->left->ExecutePostOrder();
    if (this->right)
        this->right->ExecutePostOrder();
    this->ExecuteNode();
}

void Node_WriteOut::ExecuteNode()
{
	cout << "\nExecuteNode of Node_WriteOut\n";
}
