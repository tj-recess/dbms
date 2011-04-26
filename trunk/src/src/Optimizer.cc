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
			  m_pCNF(boolean), m_Stats(s), m_nGlobalPipeID(0)
{
	// Store tables in sorted fashion in m_vSortedTables
	// and the number of tables in m_nNumTables
	m_nNumTables = SortTables();
	m_nNumTables = SortAlias();
	if (m_nNumTables != -1)
	{	
		// Print tables
		PrintTableCombinations(m_nNumTables);
		// Populate m_aTableNames with real names + alias
		//PopulateTableNames();
	}

	// make vector of m_pCNF
	TokenizeAndList(m_pCNF);

	#ifdef _DEBUG_OPTIMIZER
	PrintTokenizedAndList();
	#endif
}

Optimizer::~Optimizer()
{
	// some cleanup if needed
	// m_mJoinEstimate map
//	if (m_aTableNames)
//	{
//		delete [] m_aTableNames;
//		m_aTableNames = NULL;
//	}
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
			string sAlias = map_itr->first;
			string sTableName = map_itr->second;
			// Push alias name in the vector
            m_vSortedAlias.push_back(sAlias);

            // Make attributes to create select file node
            string sInFile = sTableName + ".bin";
            int outPipeId = m_nGlobalPipeID++;
            CNF * pCNF = new CNF();
            Record * pLit = new Record();
            Schema schema_obj("catalog", (char*)sTableName.c_str());
            AndList * new_AndList = GetSelectionsFromAndList(sAlias);
            #ifdef _DEBUG_OPTIMIZER
            PrintAndList(new_AndList);
//            TokenizeAndList(new_AndList);
//            PrintTokenizedAndList();
            m_vWholeCNF.clear();
            #endif
            pCNF->GrowFromParseTree(new_AndList, &schema_obj, *pLit);
            QueryPlanNode * pNode = new Node_SelectFile(sInFile, outPipeId, pCNF, pLit);

			// Apply "select" CNF on it and push the result in map
            Statistics * pStats = new Statistics(m_Stats);
			// pStats->Apply(new_AndList, {this alias name as char *[]}, 1)

            pair <Statistics *, QueryPlanNode *> stats_node_pair;			
            stats_node_pair.first = pStats;
            stats_node_pair.second = pNode;
			m_mJoinEstimate[sAlias] = stats_node_pair; 
        }
		
		#ifdef _ESTIMATOR_DEBUG
		cout << "\n--- map thingie ---\n";
		map <string, pair <Statistics *, QueryPlanNode*> >::iterator itr;
		itr = m_mJoinEstimate.begin();
		for (; itr != m_mJoinEstimate.end(); itr++)
		{
			cout << itr->first << " : ";
			pair <Statistics *, QueryPlanNode*> pp = itr->second;
			cout << pp.second->m_nOutPipe << endl;
		}
		#endif

        return m_vSortedAlias.size();
    }
}

// Put all table names and alias in char*[] as needed by estimate
/*void Optimizer::PopulateTableNames()
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
}*/

// Put all table names from vector to m_aTableNames
// which is a char*[] as needed by estimate
void Optimizer::PopulateTableNames(vector <string> & rel_vec)
{
	if (m_aTableNames)
	{
		delete [] m_aTableNames;
		m_aTableNames = NULL;
	}

	int size = rel_vec.size();
    m_aTableNames = new char*[size];

	for (int i = 0; i < size; i++)
    {
        // copy name
        int len = strlen(rel_vec.at(i).c_str());
        char * name = new char [len + 1];
        strcpy(name, rel_vec.at(i).c_str());
        name[len] = '\0';
        m_aTableNames[i] = name;
    }

    #ifdef _DEBUG_OPTIMIZER
    cout << "\n\n---------- names and alias ---------\n";
    for (int i = 0; i < size; i++)
        cout << m_aTableNames[i] << " ";

    cout << "\n--- done ---\n";
    #endif
}

// break sCombo apart and put table names in the vector
void Optimizer::ComboToVector(string sCombo, vector <string> & rel_vec)
{
	string sTemp = sCombo;
	int dotPos = sTemp.find(".");
	while (dotPos != string::npos)
	{
		rel_vec.push_back(sTemp.substr(0, dotPos));
		sTemp = sTemp.substr(dotPos+1);
		dotPos = sTemp.find(".");
	}
	rel_vec.push_back(sTemp);

	#ifdef _DEBUG_OPTIMIZER
	cout << "\n--- [ComboToSet] ---\nOriginal string: " << sCombo.c_str();
	cout << "\nVector data: \n";
	for (int i = 0; i < rel_vec.size(); i++)
		cout << rel_vec.at(i) << endl;
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
// nested for loop over m_vSortedAlias to make combos of 2 tables
void Optimizer::TableComboBaseCase(vector <string> & vTempCombos)
{
    cout << "\n\n--- Table combinations ---\n";
    for (int i = 0; i < m_nNumTables; i++)
    {
        for (int j = i+1; j < m_nNumTables; j++)
        {
			string sLeftTable = m_vSortedAlias.at(i);
			string sRightTable = m_vSortedAlias.at(j);

            string sName = sLeftTable + "." + sRightTable;
            cout << sName.c_str() << endl;

			// Make attributes to create Join node
			int in_pipe_left = m_mJoinEstimate[sLeftTable].second->m_nOutPipe;
			int in_pipe_right = m_mJoinEstimate[sRightTable].second->m_nOutPipe;
			int out_pipe = m_nGlobalPipeID++;
			CNF * pCNF = new CNF();
            Record * pLit = new Record();
		
			#ifdef _ESTIMATOR_DEBUG
			cout << sLeftTable.c_str() << "---" << sRightTable.c_str();
			#endif
			Schema LeftSchema("catalog", (char*)sLeftTable.c_str());
			Schema RightSchema("catalog", (char*)sRightTable.c_str());

			vector <string> vec_rel_names;
			ComboToVector(sName, vec_rel_names);	// breaks sName apart and fills up the vector

			AndList * new_AndList = GetJoinsFromAndList(vec_rel_names);
			#ifdef _DEBUG_OPTIMIZER
                        TokenizeAndList(new_AndList);
                        PrintTokenizedAndList();
                        m_vWholeCNF.clear();
			#endif
			pCNF->GrowFromParseTree(new_AndList, &LeftSchema, &RightSchema, *pLit);

			QueryPlanNode * pNode = NULL;//new Node_Join();
			Statistics * pStats = new Statistics(m_Stats);

			// TODO
			// -- Find AndList related to these 2 tables
			// new_AndList = find_new_AndList(m_vSortedAlias.at(i), m_vSortedAlias.at(j));
			//
			// Statistics * pStats = new Statistics(m_Stats);
			// -- call pStats->Apply(new_AndList, {these 2 table names}, 2);
			//			--> change distinct values
			// -- call pStats->Estimate(new_AndList, {these 2 table names}, 2);
			//			--> apply select condition before join condition
			// Use the result of estimate anywhere?
			// 


			// make stats + node pair and push in the map
			pair <Statistics *, QueryPlanNode *> stats_node_pair;
			stats_node_pair.first = pStats;
			stats_node_pair.second = pNode;
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
            loc = ComboAfterCurrTable(vTempCombos, m_vSortedAlias.at(i));
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

                    string sName = m_vSortedAlias.at(i) + "." + vTempCombos.at(j);
                    cout << sName.c_str() << endl;
					m_mJoinEstimate[sName] = stats_node_pair;    	// Push pStats into this map 
                    vNewCombo.push_back(sName);

					vector <string> temp_vec;
					ComboToVector(sName, temp_vec);
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


AndList* Optimizer::GetSelectionsFromAndList(string alias)
{
#ifdef _DEBUG_OPTIMIZER
    cout << "\n Selections for \'" << alias << "\' are : ";
#endif
    AndList* newAndList = NULL;
    AndList* parseTree = m_pCNF;   //copy to ensure boolean is intact
    AndList* prvsNode = NULL;   //useful when parseTree is splitted
    while(parseTree != NULL)
    {
        bool skipped = false;
        OrList* theOrList = parseTree->left;
        while(theOrList != NULL) //to ensure if parse tree contains an OrList and a comparisonOp
        {
            ComparisonOp* theComparisonOp = theOrList->left;
            if(theComparisonOp == NULL)
                break;
            //first push the left value
            int leftCode = theComparisonOp->left->code;
            string leftVal = theComparisonOp->left->value;


            //and now the right value
            int rightCode = theComparisonOp->right->code;
            string rightVal = theComparisonOp->right->value;


            //rules: in 1st iteration check for selection
            // in 2nd iteration check for joins
            // in any iteration if there is any alias which is not passed, skip the whole AndList i.e. break while and set flag=false
             /*
             *1. if both leftCode and rightCode is NAME, skip the AndList
             */
            if(leftCode == NAME && rightCode == NAME)   //skip joins
            {
                skipped = true;
                break;
            }
            //get alias from left and right
            //match with alias1 and alias2 passed in the function
            size_t leftDotIndex = leftVal.find(".");
            size_t rightDotIndex = rightVal.find(".");
            if(leftVal.compare(0,leftDotIndex, alias) != 0 && rightVal.compare(0,rightDotIndex, alias) != 0 )
            {
                skipped = true;
                break;
            }


            //move to next orList inside first AND;
            theOrList = theOrList->rightOr;
        }


        prvsNode = parseTree;
        parseTree = parseTree->rightAnd;
        //if lastAndList was not skipped, append it to newAndList which will be returned
        if(!skipped)
        {
//                prvsNode = parseTree->rightAnd;   //skip the node which we are gonna detach


            //update head if first node is removed
            if(prvsNode == m_pCNF)
                m_pCNF = parseTree;


            while(newAndList != NULL)
                newAndList = newAndList->rightAnd;
            newAndList = prvsNode;
            newAndList->rightAnd = NULL;


//                parseTree = prvsNode;     //increment parseTree to point to next AndList
//                prvsNode = NULL;
        }
    }
    return newAndList;
}


AndList* Optimizer::GetJoinsFromAndList(vector<string>& aliases)
{
    AndList* newAndList = NULL;
    AndList* parseTree = m_pCNF;   //copy to ensure boolean is intact
    AndList* prvsNode = NULL;   //useful when parseTree is splitted
    while(parseTree != NULL)
    {
        bool skipped = false;
        OrList* theOrList = parseTree->left;
        while(theOrList != NULL) //to ensure if parse tree contains an OrList and a comparisonOp
        {
            ComparisonOp* theComparisonOp = theOrList->left;
            if(theComparisonOp == NULL)
                break;
            //first push the left value
            int leftCode = theComparisonOp->left->code;
            string leftVal = theComparisonOp->left->value;


            //and now the right value
            int rightCode = theComparisonOp->right->code;
            string rightVal = theComparisonOp->right->value;


            //accept only joins
            if(leftCode != NAME || rightCode != NAME)
            {
                skipped = true;
                break;
            }
            
            //get alias from left and right and match with those in vector,
            //if both match, then only accept the AndList
            size_t leftDotIndex = leftVal.find(".");
            size_t rightDotIndex = rightVal.find(".");
            
            string leftAlias = leftVal.substr(0, leftDotIndex);
            string rightAlias = rightVal.substr(0, rightDotIndex);


            //skip this AndList if any of left or right alias doesn't match
            bool leftMatched = false, rightMatched = false; //expecting not to match
            for(int i = 0; i < aliases.size(); i++)
            {
                if(aliases.at(i).compare(leftAlias) == 0)
                {
                    leftMatched = true;
                }
                if(aliases.at(i).compare(rightAlias) == 0)
                {
                    rightMatched = true;
                }
            }
            if(!(leftMatched && rightMatched))
            {
                skipped = true;
                break;
            }


            //move to next orList inside first AND;
            theOrList = theOrList->rightOr;
        }


        prvsNode = parseTree;
        parseTree = parseTree->rightAnd;
        //if lastAndList was not skipped, append it to newAndList which will be returned
        if(!skipped)
        {
//                prvsNode = parseTree->rightAnd;   //skip the node which we are gonna detach


            //update head if first node is removed
            if(prvsNode == m_pCNF)
                m_pCNF = parseTree;


            while(newAndList != NULL)
                newAndList = newAndList->rightAnd;
            newAndList = prvsNode;
            newAndList->rightAnd = NULL;


//                parseTree = prvsNode;     //increment parseTree to point to next AndList
//                prvsNode = NULL;
        }
    }
    return newAndList;
}

// Break the m_pCNF apart into tokens
// Format:
// type left_value operator type right_value OR type left_value operator type right_value
// AND
// type left_value operator type right_value OR type left_value operator type right_value .
// NOTE: the "." at the end signifies the end
void Optimizer::TokenizeAndList(AndList* someParseTree)
{
    //first create copy of passed AndList
    AndList* parseTree = someParseTree;

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

void Optimizer::PrintOperand(struct Operand *pOperand)
{
        if(pOperand!=NULL)
        {
                cout<<pOperand->value<<" ";
        }
        else
                return;
}

void Optimizer::PrintComparisonOp(struct ComparisonOp *pCom)
{
        if(pCom!=NULL)
        {
                PrintOperand(pCom->left);
                switch(pCom->code)
                {
                        case 1:
                                cout<<" < "; break;
                        case 2:
                                cout<<" > "; break;
                        case 3:
                                cout<<" = ";
                }
                PrintOperand(pCom->right);

        }
        else
        {
                return;
        }
}
void Optimizer::PrintOrList(struct OrList *pOr)
{
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp(pCom);

                if(pOr->rightOr)
                {
                        cout<<" OR ";
                        PrintOrList(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}
void Optimizer::PrintAndList(struct AndList *pAnd)
{
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList(pOr);
                if(pAnd->rightAnd)
                {
                        cout<<" AND ";
                        PrintAndList(pAnd->rightAnd);
                }
        }
        else
        {
                return;
        }
}