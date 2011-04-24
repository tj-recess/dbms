#include "Estimator.h"
using namespace std;

Estimator::Estimator() : m_pFuncOp(NULL), m_pTblList(NULL), m_nNumTables(-1)
{}

Estimator::Estimator(struct FuncOperator *finalFunction,
					 struct TableList *tables)
			: m_pFuncOp(finalFunction), m_pTblList(tables)
{
	// Store tables in sorted fashion in m_vSortedTables
	// and the number of tables in m_nNumTables
	m_nNumTables = SortTables();
	if (m_nNumTables != -1)
		PrintTableCombinations(m_nNumTables);
}

Estimator::~Estimator()
{
	// some cleanup if needed
}

int Estimator::SortTables()
{
	if (m_pTblList == NULL)
		return -1;
	else
	{
	    /* Logic:
    	    1. Put table names in a map --> will get sorted automagically
        	2. Take them out and put in a vector (easier to iterate over)
    	*/

	    // Step 1
    	struct TableList *p_TblList = m_pTblList;
	    map <string, int> table_map;
    	while (p_TblList != NULL)
	    {
    	    table_map[p_TblList->tableName] = 1;
        	p_TblList = p_TblList->next;
	    }
	
    	// Step 2
	    map <string, int>::iterator map_itr = table_map.begin();
    	for (; map_itr != table_map.end(); map_itr++)
	    {
    	    m_vSortedTables.push_back(map_itr->first);
	    }
		return m_vSortedTables.size();
	}
}

void Estimator::PrintFuncOperator()
{
/* Structure of FuncOperator
   -------------------------

            NODE
            - code [0: leaf node, 42: *, 43: +, 45: -, 47: /]
            - leftOperand
                - code [1: double, 2: int, 3: name, 4: string, 5: less_than, 6: greater_than, 7: equals]
                - value
            - leftOperator (left child)
            - right (right child)

            example: (a.b + b)
    
            code: +
            leftOperand: NULL
            leftOperator        right
                /                 \
               /                   \
        code: 0                 code: 0
        leftOperand             leftOperand
            - name a.b              - name b
        leftOperator: NULL      leftOperator: NULL
        right: NULL             right: NULL         

*/
        if (m_pFuncOp == NULL)
        {
            cout << "finalFunction is null" <<endl;
            return;
        }
		else
		{
    		cout << "\n\n--- Function Operator Tree ---\n";
			PrintFuncOpRecur(m_pFuncOp);
		}
}

void Estimator::PrintFuncOpRecur(struct FuncOperator * func_node)
{
        static int pfo2_i = 1;
        struct FuncOperator *temp;
        temp = func_node;

		if (func_node == NULL)
			return;

        cout << "Node = "<< pfo2_i++ <<endl;
        // if code = 0, indicates it is a leaf node, otherwise as follows:
        cout << "\t[0: leaf node, 42: *, 43: +, 45: -, 47: / ]\n";
        cout << "\t Code = " << temp->code << endl;
        cout << "\t FuncOperand:" << endl;
        if(temp->leftOperand != NULL)
        {
            cout << "\t\t[1: double, 2: int, 3: name, 4: string, 5: less_than, 6: greater_than, 7: equals]\n";
            cout << "\t\t Code: " << temp->leftOperand->code << endl;
            cout << "\t\t Value: " << temp->leftOperand->value << endl;
        }
        else
            cout << "\t\tNULL" << endl; // NULL indicates it has children

        PrintFuncOpRecur(temp->leftOperator);  // print left subtree
        PrintFuncOpRecur(temp->right);         // print right subtree
}


void Estimator::PrintTableList()
{
	if(m_pTblList == NULL)
    	cout << "tables specified is null" << endl;

    cout << "\n\n--- Table List ---\n";
    struct TableList *tempTableList = m_pTblList;
    int i = 1;
    while(tempTableList != NULL)
    {
        cout << "Node = "<< i++ <<endl;
        cout << "\t Table Name: "<< tempTableList->tableName <<endl;
        cout << "\t Alias As: "<< tempTableList->aliasAs <<endl;

        tempTableList = tempTableList->next;
    }
}


// handles the base case when we have to find 2 table combos
// nested for loop over m_vSortedTables to make combos of 2 tables
void Estimator::TableComboBaseCase(vector <string> & vTempCombos)
{
    cout << "\n\n--- Table combinations ---\n";
    for (int i = 0; i < m_nNumTables; i++)
    {
        for (int j = i+1; j < m_nNumTables; j++)
        {
            string sName = m_vSortedTables.at(i) + "." + m_vSortedTables.at(j);
            cout << sName.c_str() << endl;
			m_mJoinEstimate[sName] = 0;		// Push this join in map
            vTempCombos.push_back(sName);
        }
    }
    cout << endl;
}

/* ABCDE

    2 combos:
    AB AC AD AE
    BC BD BE
    CD CE
    DE

    3 combos:
    A (find location after all A.x finish) --> BC BD BE CD CE DE
    B (find location after all B.x finish) --> CD CE DE
    C (find location after all C.x finish) --> DE
    D (find location after all D.x finish) --> none
    E (find location after all E.x finish) --> none

    ==> ABC ABD ABE ACD ACE ADE BCD BCE BDE CDE

    4 combos: 
    A (find location after all A.x finish) --> BCD BCE BDE
    B (find location after all B.x finish) --> CDE
    C (find location after all C.x finish) --> none
    D (find location after all D.x finish) --> none
    E (find location after all E.x finish) --> none

*/

// recursive function to find all combinations of table
vector<string> Estimator::PrintTableCombinations(int combo_len)
{
    vector <string> vTempCombos, vNewCombo;

	// base case: combinations of 2 tables
    if (combo_len == 2)
    {
        TableComboBaseCase(vTempCombos);
        //cout <<  vTempCombos.size() << endl;
        return vTempCombos;
    }
    else
    {
        vTempCombos = PrintTableCombinations(combo_len-1);
        //cout << vTempCombos.size() << endl;
        for (int i = 0; i < m_nNumTables; i++)
        {
			int loc = -1;
            loc = ComboAfterCurrTable(vTempCombos, m_vSortedTables.at(i));
            // if lec = -1 --> error
            if (loc != -1)
            {
				// Append current table with the other combinations (see logic above)
				int len = vTempCombos.size();
                for (int j = loc; j < len; j++)
                {
                    string sName = m_vSortedTables.at(i) + "." + vTempCombos.at(j);
                    cout << sName.c_str() << endl;
					m_mJoinEstimate[sName] = 0;     // Push this join in map				
                    vNewCombo.push_back(sName);
                }
            }
        }
        return vNewCombo;
    }
}

// Find the location where "sTableToFind" stops being the 1st table
// Logic: sTableToFind should appear first at least once, then stop
int Estimator::ComboAfterCurrTable(vector<string> & vTempCombos, string sTableToFind)
{
    // example: vTempCombos = A.B A.C A.D B.C B.D C.D
    //          sTableToFind = A, return 3
    //          sTableToFind = B, return 5
    //          sTableToFind = C, return -1
    int len = vTempCombos.size();
    int dotPos;
    bool bFoundOnce = false;
    for (int i = 0; i < len; i++)
    {
        string sTab = vTempCombos.at(i);
        dotPos = sTab.find(".");
        if (dotPos == string::npos)
            return -1;              // "." not found... error
        else
        {
            string sFirstTab = sTab.substr(0, dotPos);
            if (sFirstTab.compare(sTableToFind) == 0)
                bFoundOnce = true;
            else if (bFoundOnce == true)
                return i;
            else
                continue;
        }
    }
    return -1;
}


