#include "Optimizer.h"
using namespace std;

Optimizer::Optimizer() : m_pFuncOp(NULL), m_pTblList(NULL), m_pCNF(NULL),
						 m_aTableNames(NULL), m_nNumTables(-1), m_nGlobalPipeID(0)
{}

Optimizer::Optimizer(struct FuncOperator *finalFunction,
					 struct TableList *tables,
					 struct AndList * boolean,
					 Statistics & s)
			: m_pFuncOp(finalFunction), m_pTblList(tables),
			  m_pCNF(boolean), m_Stats(s)
{
	// Store tables in sorted fashion in m_vSortedTables
	// and the number of tables in m_nNumTables
	m_nNumTables = SortTables();
	if (m_nNumTables != -1)
	{	
		// Print tables
		PrintTableCombinations(m_nNumTables);
		// Populate m_aTableNames with real names + alias
		PopulateTableNames();
	}

	// make vector of m_pCNF
	TokenizeAndList();

	#ifdef _ESTIMATOR_DEBUG
	PrintTokenizedAndList();
	#endif
}

Optimizer::~Optimizer()
{
	// some cleanup if needed
	// m_mJoinEstimate map
}

int Optimizer::SortTables()
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

// Sort alias and push them in m_vSortedAlias vector
int Optimizer::SortAlias()
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
        map <string, string> table_map;
        while (p_TblList != NULL)
        {
			// Make a copy of this table name AS alias name
			m_Stats.CopyRel(p_TblList->tableName, p_TblList->aliasAs);
			// Push the alias name in map with original table name as value
			// Reason, we need the original table name to find the .bin file
            table_map[p_TblList->aliasAs] = p_TblList->tableName;
            p_TblList = p_TblList->next;
        }

        // Step 2
        map <string, string>::iterator map_itr = table_map.begin();
        for (; map_itr != table_map.end(); map_itr++)
        {
			// Push alias name in the vector
            m_vSortedAlias.push_back(map_itr->first);

			// Make attributes to create select file node
			string sInFile = map_itr->second + ".bin";
			int outPipeId = m_nGlobalPipeID++;
			CNF * pCNF = new CNF();
			Record * pLit = new Record();
			Schema schema_obj("catalog", (char*)map_itr->second.c_str());
//			pCNF->GrowFromParseTree(new_AndList, schema_obj, *pLit);
            QueryPlanNode * pNode = new Node_SelectFile(sInFile, outPipeId, pCNF, pLit);

			// Apply "select" CNF on it and push the result in map
            Statistics * pStats = new Statistics(m_Stats);
			// pStats->Apply(new_AndList, {this alias name as char *[]}, 1)

            pair <Statistics *, QueryPlanNode *> stats_node_pair;			
            stats_node_pair.first = pStats;
            stats_node_pair.second = pNode;
			m_mJoinEstimate[map_itr->first] = stats_node_pair; 
        }
        return m_vSortedAlias.size();
    }
}

// Put all table names and alias in char*[] as needed by estimate
void Optimizer::PopulateTableNames()
{
	m_aTableNames = new char*[m_nNumTables*2];
	struct TableList *p_TblList = m_pTblList;
	int len, j = 0;
	while (p_TblList != NULL)
    {
		// copy name
		len = strlen(p_TblList->tableName);
		char * name = new char [len + 1];
		strcpy(name, p_TblList->tableName);
		name[len] = '\0';
		m_aTableNames[j++] = name;

		// copy alias
		len = strlen(p_TblList->aliasAs);
		char * alias = new char [len + 1];
		strcpy(alias, p_TblList->aliasAs);
		alias[len] = '\0';
		m_aTableNames[j++] = alias;

		p_TblList = p_TblList->next;
	}

	#ifdef _ESTIMATOR_DEBUG
	cout << "\n\n---------- names and alias ---------" << j << "\n";
	for (int i = 0; i < j; i++)
		cout << m_aTableNames[i] << " "; 

	cout << "\n--- done ---\n";
	#endif
}

void Optimizer::PrintFuncOperator()
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

void Optimizer::PrintFuncOpRecur(struct FuncOperator * func_node)
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


void Optimizer::PrintTableList()
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
void Optimizer::TableComboBaseCase(vector <string> & vTempCombos)
{
    cout << "\n\n--- Table combinations ---\n";
    for (int i = 0; i < m_nNumTables; i++)
    {
        for (int j = i+1; j < m_nNumTables; j++)
        {
			pair <Statistics *, QueryPlanNode *> stats_node_pair;
			Statistics * pStats = new Statistics(m_Stats);
			QueryPlanNode * pNode = NULL; 
			stats_node_pair.first = pStats;
			stats_node_pair.second = pNode;

			// TODO
			// -- Find AndList related to these 2 tables
			// new_AndList = find_new_AndList(m_vSortedTables.at(i), m_vSortedTables.at(j));
			//
			// Statistics * pStats = new Statistics(m_Stats);
			// -- call pStats->Apply(new_AndList, {these 2 table names}, 2);
			//			--> change distinct values
			// -- call pStats->Estimate(new_AndList, {these 2 table names}, 2);
			//			--> apply select condition before join condition
			// Use the result of estimate anywhere?
			// 

            string sName = m_vSortedTables.at(i) + "." + m_vSortedTables.at(j);
            cout << sName.c_str() << endl;
			m_mJoinEstimate[sName] = stats_node_pair;		// Push pStats into this map
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
vector<string> Optimizer::PrintTableCombinations(int combo_len)
{
    vector <string> vTempCombos, vNewCombo;

	// base case: combinations of 2 tables
    if (combo_len == 2)
    {
        TableComboBaseCase(vTempCombos);
        return vTempCombos;
    }
    else
    {
        vTempCombos = PrintTableCombinations(combo_len-1);
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
					pair <Statistics *, QueryPlanNode *> stats_node_pair;
			        Statistics * pStats = new Statistics(m_Stats); // <-- use m_stats of vTempCombos.at(j) tables
            		QueryPlanNode * pNode = NULL; 
			        stats_node_pair.first = pStats;
            		stats_node_pair.second = pNode;

					// TODO
					// Make char ** with all the table names being used here
					// Find AndList with all these tables
					// Statistics * pStats = new Statistics(m_Stats);
		            // -- call pStats->Apply(new_AndList, {these n table names}, n);
					//			--> somehow use the stats from the map for 2-table base combos
        		    //          --> change distinct values
		            // -- call pStats->Estimate(new_AndList, {these n table names}, n);
        		    //          --> apply select condition before join condition
		            // Use the result of estimate anywhere?

                    string sName = m_vSortedTables.at(i) + "." + vTempCombos.at(j);
                    cout << sName.c_str() << endl;
					m_mJoinEstimate[sName] = stats_node_pair;    	// Push pStats into this map 
                    vNewCombo.push_back(sName);
                }
            }
        }
        return vNewCombo;
    }
}

// Find the location where "sTableToFind" stops being the 1st table
// Logic: sTableToFind should appear first at least once, then stop
int Optimizer::ComboAfterCurrTable(vector<string> & vTempCombos, string sTableToFind)
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

// Break the m_pCNF apart into tokens
// Format:
// type left_value operator type right_value OR type left_value operator type right_value
// AND
// type left_value operator type right_value OR type left_value operator type right_value .
// NOTE: the "." at the end signifies the end
void Optimizer::TokenizeAndList()
{
    //first create copy of passed AndList
    AndList* parseTree = m_pCNF;;

    while(parseTree != NULL)
    {
        OrList* theOrList = parseTree->left;
        while(theOrList != NULL) //to ensure if parse tree contains an OrList and a comparisonOp
        {
            ComparisonOp* theComparisonOp = theOrList->left;
            if(theComparisonOp == NULL)
                break;
            //first push the left value
            int leftCode = theComparisonOp->left->code;
            string leftVal = theComparisonOp->left->value;

            m_vWholeCNF.push_back(System::my_itoa(leftCode));
            m_vWholeCNF.push_back(leftVal);   //remember to apply itoa before using double or int

            //now push the operator
            m_vWholeCNF.push_back(System::my_itoa(theComparisonOp->code));

            //and now the right value
            int rightCode = theComparisonOp->right->code;
            string rightVal = theComparisonOp->right->value;

            m_vWholeCNF.push_back(System::my_itoa(rightCode));
            m_vWholeCNF.push_back(rightVal);   //remember to apply itoa before using double or int
            //move to next orList inside first AND
            if(theOrList->rightOr != NULL)
                m_vWholeCNF.push_back("OR");
            theOrList = theOrList->rightOr;
        }

        //move to next AndList node of the parseTree
        if(parseTree->rightAnd != NULL)
            m_vWholeCNF.push_back("AND");
        else
            m_vWholeCNF.push_back(".");
        parseTree = parseTree->rightAnd;
    }	
}

void Optimizer::PrintTokenizedAndList()
{
	cout << "\n\n-------- Tokenized AND-list ----------\n";
	for (int i = 0; i < m_vWholeCNF.size(); i++)
		cout << m_vWholeCNF.at(i) << "  ";
}
