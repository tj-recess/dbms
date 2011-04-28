#include "Optimizer.h"
using namespace std;

Optimizer::Optimizer() : m_pFuncOp(NULL), m_pTblList(NULL), m_pCNF(NULL),
						 m_pGroupingAtts(NULL), m_pAttsToSelect(NULL),
						 m_nDistinctAtts(0), m_nDistinctFunc(0),
						 m_nNumTables(-1), m_nGlobalPipeID(0), m_aTableNames(NULL)
{}

Optimizer::Optimizer(struct FuncOperator *finalFunction,
					 struct TableList *tables,
					 struct AndList * boolean,
					 struct NameList * pGrpAtts,
					 struct NameList * pAttsToSelect,
					 int distinct_atts, int distinct_func,
					 Statistics & s)
			: m_pFuncOp(finalFunction), m_pTblList(tables), m_pCNF(boolean), 
			  m_pGroupingAtts(pGrpAtts), m_pAttsToSelect(pAttsToSelect), 
			  m_nDistinctAtts(distinct_atts), m_nDistinctFunc(distinct_func),
			  m_nNumTables(-1), m_nGlobalPipeID(0), m_aTableNames(NULL), m_Stats(s)
{
	// Store alias in sorted fashion in m_vSortedAlias
	// and the number of tables/alias in m_nNumTables
	//m_nNumTables = SortTables();
	m_nNumTables = SortAlias();
	if (m_nNumTables != -1)
	{	
		// Print tables
		m_vTabCombos = PrintTableCombinations(m_nNumTables);
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
			// Make a copy of this table name AS alias name in Stats object
			m_Stats.CopyRel(p_TblList->tableName, p_TblList->aliasAs);

			// make an entry in the map alias --> orig table
			m_mAliasToTable[p_TblList->aliasAs] = p_TblList->tableName;

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
            CNF * pCNF = NULL;
            Record * pLit = NULL;
            Schema schema_obj("catalog", (char*)sTableName.c_str());
            AndList * new_AndList = GetSelectionsFromAndList(sAlias);
            #ifdef _DEBUG_OPTIMIZER
            		PrintAndList(new_AndList);
		            cout << "\n";
            #endif
            QueryPlanNode * pNode = NULL;
            if (new_AndList != NULL)
            {
                // Apply "select" CNF on it and push the result in map
                char* relNames[] = { const_cast<char*>(sAlias.c_str()) };
                vector <string> vec_rels;
                vec_rels.push_back(sAlias);
                vec_rels.push_back(m_mAliasToTable[sAlias]);
                PopulateTableNames(vec_rels);
                m_Stats.Apply(new_AndList, m_aTableNames, 2);

                pCNF = new CNF();
                pLit = new Record();

                //remove dots from column name in the selected node
                RemoveAliasFromColumnName(new_AndList);
    	        pCNF->GrowFromParseTree(new_AndList, &schema_obj, *pLit);
            }
            // Now create the SelectFile Node
       	    pNode = new Node_SelectFile(sInFile, outPipeId, pCNF, pLit);

            // push outPipe --> combo name in the map
            m_mOutPipeToCombo[outPipeId] = sAlias;

//            pair <Statistics *, QueryPlanNode *> stats_node_pair;
//            stats_node_pair.first = &m_Stats;
//            stats_node_pair.second = pNode;
//            m_mJoinEstimate[sAlias] = stats_node_pair;
            JoinValue * jv = new JoinValue;
            jv->stats = &m_Stats;
            jv->queryPlanNode = pNode;
            jv->joinOrder = sAlias;  //default join order for singleton (unjoined) table
//            jv.schema = ;
            m_mJoinEstimate[sAlias] = *jv;
        }
		
		#ifdef _DEBUG_OPTIMIZER 
		//print_map();
		#endif

        return m_vSortedAlias.size();
    }
}

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
	/*cout << "\n--- [ComboToSet] ---\nOriginal string: " << sCombo.c_str();
	cout << "\nVector data: \n";
	for (int i = 0; i < rel_vec.size(); i++)
		cout << rel_vec.at(i) << endl;
    cout << "\n--- done ---\n";*/
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

// Only for debugging
void Optimizer::print_map()
{
        cout << "\n--- map thingie ---\n";
        map <string, JoinValue >::iterator itr;
        itr = m_mJoinEstimate.begin();
        for (; itr != m_mJoinEstimate.end(); itr++)
        {
            cout << itr->first << " : ";
            JoinValue jv = itr->second;
			if (jv.stats != NULL)
	            cout << jv.stats->GetPartitionNumber() << endl;
			else
				cout << "No Join!\n";
        }
}


// handles the base case when we have to find 2 table combos
// nested for loop over m_vSortedAlias to make combos of 2 tables
void Optimizer::TableComboBaseCase(vector <string> & vTempCombos)
{
	#ifdef _DEBUG_OPTIMIZER
	//print_map();
	#endif

    cout << "\n\n--- Table combinations ---\n";
    for (int i = 0; i < m_nNumTables; i++)
    {
        for (int j = i+1; j < m_nNumTables; j++)
        {
			string sLeftAlias = m_vSortedAlias.at(i);
			string sRightAlias = m_vSortedAlias.at(j);

            string sName = sLeftAlias + "." + sRightAlias;
            cout << sName.c_str() << endl;

            int in_pipe_left;
            int in_pipe_right;
            int out_pipe;
            CNF * pCNF = NULL;
            Record * pLit = NULL;
			Schema * pSchema = NULL;

            vector <string> vec_rel_names;
            ComboToVector(sName, vec_rel_names);	// breaks sName apart and fills up the vector

            AndList * new_AndList = GetJoinsFromAndList(vec_rel_names);
            #ifdef _DEBUG_OPTIMIZER
            TokenizeAndList(new_AndList);
            PrintTokenizedAndList();
            m_vWholeCNF.clear();
            #endif

            if(new_AndList != NULL)
            {
                //store this new_AndList against alias in m_mAliasToAndList map
                m_mAliasToAndList[sName] = new_AndList;
                #ifdef _DEBUG_OPTIMIZER
                cout << "\nInserted into map -- \n" << sName << " : ";
                PrintAndList(new_AndList);
                #endif

                //store this AndList in a map and mark as unused
                m_mAndListToUsage[new_AndList] = false;
                #ifdef _DEBUG_OPTIMIZER
                cout << "\nInserted into map -- \n";
                PrintAndList(new_AndList);
                cout << " : " << false;
                #endif
            }

            QueryPlanNode * pNode = NULL;
            Statistics * pStats = NULL;
			string sOptimalOrder;
            if (new_AndList != NULL)
            {
                    // copy alias as well as table names in a new vector
                    vector <string> rel_names;
                    for (int i = 0; i < vec_rel_names.size(); i++)
                    {
                            rel_names.push_back(vec_rel_names.at(i));
                            rel_names.push_back(m_mAliasToTable[vec_rel_names.at(i)]);
                    }
                    // This function will fill up m_aTableNames
	            	PopulateTableNames(rel_names);
                    
                    //get optimal order, call stats object from the map accordingly, apply on that stats object
					pair<string, string> optimalPair;
                    FindOptimalPairing(vec_rel_names, new_AndList, optimalPair);

					sOptimalOrder = optimalPair.first + "." + optimalPair.second;

					#ifdef _DEBUG_OPTIMIZER
			        //cout << "\n-- Optimal Order found (outside): " << optimalPair.first.c_str() << ", "
            		//	 << optimalPair.second.c_str() << endl;
					#endif

					// Fetch the stats object for the optimal pair, and make a copy of it
					string sKey(optimalPair.second);
					JoinValue tmp_jv =  m_mJoinEstimate[sKey];
					pStats = new Statistics(*tmp_jv.stats);
                    
                    // Make attributes to create Join node
                    in_pipe_left = m_mJoinEstimate[optimalPair.first].queryPlanNode->m_nOutPipe;
                    in_pipe_right = m_mJoinEstimate[optimalPair.second].queryPlanNode->m_nOutPipe;
                    out_pipe = m_nGlobalPipeID++;

                    Schema LeftSchema("catalog", (char*) m_mAliasToTable[sLeftAlias].c_str());
                    Schema RightSchema("catalog", (char*) m_mAliasToTable[sRightAlias].c_str());

				// Now call apply
        	    pStats->Apply(new_AndList, m_aTableNames, rel_names.size());
				// TODO
				// change distinct values
	            pCNF = new CNF();
	            pLit = new Record();

                //remove dots from column name in the selected node
                RemoveAliasFromColumnName(new_AndList);
                pCNF->GrowFromParseTree(new_AndList, &LeftSchema, &RightSchema, *pLit);

				// Read data from left schema and right schema and write to sName.schema file
                ConcatSchemas(&LeftSchema, &RightSchema, sName);
                pSchema = new Schema((char*)(sName + ".schema").c_str(), (char*)sName.c_str());
            }
			// Then create the Join Node	
			pNode = new Node_Join(in_pipe_left, in_pipe_right, out_pipe, pCNF, pSchema, pLit);

			// push outPipe --> combo name in the map
			m_mOutPipeToCombo[out_pipe] = sName;

			// make stats + node + optimal order and push in the map
			JoinValue * jv = new JoinValue;
			jv->stats = pStats;
			jv->queryPlanNode = pNode;
			jv->joinOrder = sOptimalOrder;
			//                        jv.schema = ;

			m_mJoinEstimate[sName] = *jv;		// Push in the map
            vTempCombos.push_back(sName);		// Push this combination in the combination-vector
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

	// if there is only one table
	if (combo_len == 1)
	{
		vTempCombos.push_back(m_vSortedAlias.at(0));
		return vTempCombos;
	}
	// base case: combinations of 2 tables
    else if (combo_len == 2)
    {
        TableComboBaseCase(vTempCombos);
        return vTempCombos;
    }
    else
    {
        vTempCombos = PrintTableCombinations(combo_len-1);

		// Malvika: Hack for 3 tables
		if (m_nNumTables == 3)
			return vTempCombos;
		
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
                    string sLeftAlias = m_vSortedAlias.at(i);
                    string sRightAlias = vTempCombos.at(j);
                    string sName = sLeftAlias + "." + sRightAlias;
                    cout << sName.c_str() << endl;

                    int in_pipe_left = m_mJoinEstimate[sLeftAlias].queryPlanNode->m_nOutPipe;
                    int in_pipe_right = m_mJoinEstimate[sRightAlias].queryPlanNode->m_nOutPipe;
                    int out_pipe = m_nGlobalPipeID++;
                    CNF * pCNF = NULL;
                    Record * pLit = NULL;
					Schema * pSchema = NULL;

                    Statistics * pStats = new Statistics(*(m_mJoinEstimate[sRightAlias].stats));
                    QueryPlanNode * pNode = NULL;

                    // TODO
                    // Make char ** with all the table names being used here
                    vector <string> vec_rel_names;
                    ComboToVector(sName, vec_rel_names);    // breaks sName apart and fills up the vector

                    // Find AndList with all these tables
                    AndList * new_AndList = GetJoinsFromAndList(vec_rel_names);
                    #ifdef _DEBUG_OPTIMIZER
                    TokenizeAndList(new_AndList);
                    PrintTokenizedAndList();
                    m_vWholeCNF.clear();
                    #endif

                    if (new_AndList != NULL)
                    {
                        // copy alias as well as table names in a new vector
                        vector <string> rel_names;
                        for (int i = 0; i < vec_rel_names.size(); i++)
                        {
                                rel_names.push_back(vec_rel_names.at(i));
                                rel_names.push_back(m_mAliasToTable[vec_rel_names.at(i)]);
                        }
                        // This function will fill up m_aTableNamesTableNames
                        PopulateTableNames(rel_names);

                        // Now call apply
                        pStats->Apply(new_AndList, m_aTableNames, rel_names.size());
                        // TODO
                        // change distinct values
                        pCNF = new CNF();
                        pLit = new Record();

                        //create left and right Schema
						Schema LeftSchema("catalog", (char*) m_mAliasToTable[sLeftAlias].c_str());
						string right_schema_filename = sRightAlias + ".schema";
                        Schema RightSchema((char*)right_schema_filename.c_str(),
                                           (char*) sRightAlias.c_str());

                        //remove dots from column name in the selected node
                        RemoveAliasFromColumnName(new_AndList);
                        pCNF->GrowFromParseTree(new_AndList, &LeftSchema, &RightSchema, *pLit);

		                // Read data from left schema and right schema and write to sName.schema file
        		        ConcatSchemas(&LeftSchema, &RightSchema, sName);
                		pSchema = new Schema((char*)(sName + ".schema").c_str(), (char*)sName.c_str());
                    }

		            // Then create the Join Node    
        		    pNode = new Node_Join(in_pipe_left, in_pipe_right, out_pipe, pCNF, pSchema, pLit);

		            // push outPipe --> combo name in the map
        		    m_mOutPipeToCombo[out_pipe] = sName;

                    
                    JoinValue * jv = new JoinValue;
                    jv->stats = &m_Stats;
                    jv->queryPlanNode = pNode;
//                    jv.joinOrder = ;
//                    jv.schema = ;
                    m_mJoinEstimate[sName] = *jv;
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

void Optimizer::MakeQueryPlan()
{
	if (m_nNumTables > 3)
	{
		cout << "\n Cannot handle yet!\n";
		return;
	}

	QueryPlanNode * pFinalNode = NULL;
	Schema * pFinalSchema = NULL;

	if (m_nNumTables == 1)		// join is not possible
	{
		pFinalNode = m_mJoinEstimate[m_vSortedAlias.at(0)].queryPlanNode;
		if (pFinalNode == NULL)
		{
			cerr << "\nERROR: Single table doesn't have a query plan node!\n";
			return;
		}
	}

	// take care of joins
	else if (m_nNumTables == 2 || m_nNumTables == 3)
	{
		print_map();
		/* LOGIC:
		   Scan the m_mJoinEstimate map and find X.Y pairs for min estimate
		   The remaining table is Z in ...  Z join (x.Y)
		   Start backtracking
		 */
		vector <string> two_tab_combos;
		for (int i = 0; i < m_vTabCombos.size(); i++)
		{
				// push only the combinations that join "." in it
				// meaning: a.b or a.c or b.c 
				if (m_vTabCombos.at(i).find(".") != string::npos)
						two_tab_combos.push_back(m_vTabCombos.at(i));
		}

		// go over two_tab_combos and find the combo with min estimate
		double min_est = -1;
		string min_order, min_first, min_second;
		for (int i = 0; i < two_tab_combos.size(); i++)
		{
				cout << endl << two_tab_combos.at(i);
				string sCombo = two_tab_combos.at(i);
				int dotPos = sCombo.find(".");
				string sFirst = sCombo.substr(0, dotPos);
				Statistics * pStats = m_mJoinEstimate[sCombo].stats;
				if (pStats != NULL)
				{
						map<string, TableInfo> relStatsMap = (*pStats->GetRelStats());
						double est = relStatsMap[sFirst].numTuples;
						if (min_est = -1)
						{
								min_est = est;
								min_order = sCombo;
								min_first = sFirst;
								min_second = sCombo.substr(dotPos+1);
						}
						else if (min_est > est)
						{
								min_est = est;
								min_order = sCombo;
								min_first = sFirst;
								min_second = sCombo.substr(dotPos+1);
						}
				}
		}

		#ifdef _DEBUG_OPTIMIZER
		// now min est = min_est and min_combo are known
		cout << "\n\n*** " << min_est << " *** " << min_order.c_str()<<endl;
		cout << "\n\n*** " << min_first.c_str() << " *** " << min_second.c_str()<<endl;
		#endif

		// Find the 3rd table
		string min_third;
		for (int i = 0; i < m_vSortedAlias.size(); i++)
		{
				if (m_vSortedAlias.at(i).compare(min_first) != 0 &&
								m_vSortedAlias.at(i).compare(min_second) != 0)
				{
						min_third = m_vSortedAlias.at(i);
						break;
				}
		}
	
		#ifdef _DEBUG_OPTIMIZER
		cout << "\n\n*** " << min_first.c_str() << " *** " << min_second.c_str()
 			 << " *** " << min_third.c_str() << endl;
		#endif

		// break min_order apart into min_join_left and min_join_right
		string min_join_order = m_mJoinEstimate[min_order].joinOrder;
		string min_join_left = min_order.substr(0, min_order.find(".")); 
		string min_join_right = min_order.substr(min_order.find(".")+1); 


		// Only two tables to join
		if (m_nNumTables == 2)
		{
            int ip1 = m_mJoinEstimate[min_join_left].queryPlanNode->m_nOutPipe;
            int ip2 = m_mJoinEstimate[min_join_right].queryPlanNode->m_nOutPipe;
            int op = m_nGlobalPipeID++;
            // make the schema
            Schema LeftSchema("catalog", (char*) m_mAliasToTable[min_join_left].c_str());
            Schema RightSchema("catalog", (char*) m_mAliasToTable[min_join_right].c_str());
            string sName = min_order;
            // Read data from left schema and right schema and write to sName.schema file
            ConcatSchemas(&LeftSchema, &RightSchema, sName);
            pFinalSchema = new Schema((char*)(sName + ".schema").c_str(), (char*)sName.c_str());

            // Make join node
            QueryPlanNode * pFinalJoin = new Node_Join(ip1, ip2, op, NULL /* cnf */,
                                                       pFinalSchema, NULL /*record literal */);

            pFinalJoin = m_mJoinEstimate[min_order].queryPlanNode;
            pFinalJoin->left = m_mJoinEstimate[min_join_left].queryPlanNode;
            pFinalJoin->right = m_mJoinEstimate[min_join_right].queryPlanNode;

            pFinalNode = pFinalJoin;
		}
		else if (m_nNumTables == 3)
		{
			// ------ optimal join : left deep --------
			int ip1 = m_mJoinEstimate[min_order].queryPlanNode->m_nOutPipe;
			int ip2 = m_mJoinEstimate[min_third].queryPlanNode->m_nOutPipe;
			int op = m_nGlobalPipeID++;

            // Make the schema
            Schema LeftSchema("catalog", (char*) m_mAliasToTable[min_third].c_str());
                // Read the schema from min_order.schema, where table name is min_order
            Schema RightSchema((char*)(min_order + ".schema").c_str(), (char*)min_order.c_str());
            string sName = min_third + "." + min_order;
            // Read data from left schema and right schema and write to sName.schema file
            ConcatSchemas(&LeftSchema, &RightSchema, sName);
            pFinalSchema = new Schema((char*)(sName + ".schema").c_str(), (char*)sName.c_str());

            // Make the join node
            QueryPlanNode * pFinalJoin = new Node_Join(ip1, ip2, op, NULL /* cnf */,
                                                       pFinalSchema, NULL /*record literal */);

            // set child pointers of Final Join Node
            pFinalJoin->left = m_mJoinEstimate[min_order].queryPlanNode; 	// left ptr
            pFinalJoin->right = m_mJoinEstimate[min_third].queryPlanNode; 	// right ptr

            // set child pointers of min_order (intermediate order node)
            QueryPlanNode * pIntermediate = m_mJoinEstimate[min_third].queryPlanNode;
            pIntermediate->left = m_mJoinEstimate[min_join_left].queryPlanNode;		// left ptr
            pIntermediate->right = m_mJoinEstimate[min_join_right].queryPlanNode;	// right ptr

			// Set this join node as the top-most node
            pFinalNode = pFinalJoin;


			#ifdef _DEBUG_OPTIMIZER
			//print both the maps
			//1. m_mAndList to boolean
			PrintAndListToUsageMap();

			//2. m_mAliasToAndList
			PrintAliasToAndListMap();
			#endif

			//min_order -> optimal combi for join of 3
			//find value for this key in m_mAliasToAndList map and supply that to m_mAndListToUsage map and mark it as true
			//                cout << "\nmin_order = " << min_order << endl;
			m_mAndListToUsage[m_mAliasToAndList[min_order]] = true;

			//now scan rest of the map m_mAndListToUsage, concatenate all unused AndLists
			//and place them against join of 3 in m_mAliasToAndList map
			map<AndList*, bool>::iterator listToUsageIt = m_mAndListToUsage.begin();
			AndList* bigAndList = NULL;
			while(listToUsageIt != m_mAndListToUsage.end())
			{
					if(listToUsageIt->second == false)  //i.e. unused
					{
							if(bigAndList == NULL)
									bigAndList = listToUsageIt->first;
							else
							{
									//append to end of bigAndList
									AndList* temp = bigAndList;
									while(temp->rightAnd != NULL)
											temp = temp->rightAnd;
									temp->rightAnd = listToUsageIt->first;
							}

							//also mark this used AndList and used in Usage map
							m_mAndListToUsage[listToUsageIt->first] = true;
					}
					listToUsageIt++;
			}
			//now place bigAndList against join of 3 in m_mAliasToAndList map
			m_mAliasToAndList[min_order + "." + min_third] = bigAndList;
			//also place bigAndList in m_mAndListToUsage and mark as false (for future)
			//                m_mAndListToUsage[bigAndList] = false;

			#ifdef _DEBUG_OPTIMIZER
			//print both the maps
			//1. m_mAndList to boolean
			PrintAndListToUsageMap();

			//2. m_mAliasToAndList
			PrintAliasToAndListMap();
			#endif

		}
	}
    // group by
    if (m_pGroupingAtts)
    {
            OrderMaker * pGrpOrder = new OrderMaker(pFinalSchema);
            int in = pFinalNode->m_nOutPipe;
            int out = m_nGlobalPipeID++;
            QueryPlanNode * pGrpNode = new Node_GroupBy(in, out, NULL /* Function */, pGrpOrder);
            pGrpNode->left = pFinalNode;    // make join the left child of group-by
            pFinalNode = pGrpNode;          // now final node is group by (its on top!)
    }
    // sum
    // project
	if (m_pAttsToSelect)
	{
		int in = pFinalNode->m_nOutPipe;
		int out = m_nGlobalPipeID++;
		int numAttsToSelect = 0, numTotAtts = 0;
		vector <int> vec_AttsToKeepNum;
		vector <string> vec_AttsToKeepName;

		// Find num atts to keep and store names in vec_AttsToKeepName
		NameList * temp = m_pAttsToSelect;
		while (temp != NULL)
		{
			numAttsToSelect++;
			vec_AttsToKeepName.push_back(temp->name);
			temp = temp->next;
		}
		
		Schema * pTempSchema = NULL;	
		// Find total atts: sum of total atts in each table
		for (int i = 0; i < m_vSortedAlias.size(); i++)
		{
			pTempSchema = new Schema("catalog", (char*) m_mAliasToTable[m_vSortedAlias.at(i)].c_str());

			if (pTempSchema == NULL)
			{
				cerr << "\n\nERRPR! Schema for " << m_vSortedAlias.at(i) << " not found!\n\n";
				break;
			}

			// iterate over att-to-keep vector and see if that attribute belongs to this table
			// if it does, save its number (offsetted number)
			for (int j = 0; j < vec_AttsToKeepName.size(); j++)
			{
				// remove alias name from the column name
				string sFullName = vec_AttsToKeepName.at(j), sColName;
				int dotPos = sFullName.find(".");
				if (dotPos == string::npos)
					sColName = sFullName;
				else
					sColName = sFullName.substr(dotPos+1);

				// find column name in this schema
				int found_pos = pTempSchema->Find((char*)sColName.c_str());
				if (found_pos != -1)
					vec_AttsToKeepNum.push_back(numTotAtts + found_pos);
			}

			// increment num of atts count
			numTotAtts += pTempSchema->GetNumAtts();

			delete pTempSchema;			
		}

		// convert vector vec_AttsToKeepNum to int*[]
		int size = vec_AttsToKeepNum.size();
		int *attsToKeep = NULL;
		if (size != 0)
			attsToKeep = new int[size];

		for (int j = 0; j < size; j++)
			attsToKeep[j] = vec_AttsToKeepNum.at(j);

		Node_Project * pProjection = new Node_Project(in, out, attsToKeep, numAttsToSelect, numTotAtts);	
	
		pProjection->left = pFinalNode;
		// Make this node top most
		pFinalNode = pProjection;
	}

	// Print the final query plan
    if (pFinalNode!= NULL)
        pFinalNode->PrintNode();
	else
		cout << "\nERROR! No Query Plan possible!\n\n";
}

void Optimizer::ExecuteQuery()
{
    /* Logic:
     * start with parent node, perform a post-order traversal
     * execute every node encountered and join it's input and output pipe
     * according to the order mentioned in nodes
     */
//    m_pFinalJoin->()

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
            //update head if first node is removed
            if(prvsNode == m_pCNF)
                m_pCNF = parseTree;

            //remove dots from column name in the selected node
            //RemoveAliasFromColumnName(prvsNode);

            while(newAndList != NULL)
                newAndList = newAndList->rightAnd;
            newAndList = prvsNode;
            newAndList->rightAnd = NULL;
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
            //update head if first node is removed
            if(prvsNode == m_pCNF)
                m_pCNF = parseTree;

            //remove dots from column name in the selected node
            //RemoveAliasFromColumnName(prvsNode);
            
            while(newAndList != NULL)
                newAndList = newAndList->rightAnd;
            newAndList = prvsNode;
            newAndList->rightAnd = NULL;
        }
    }
    return newAndList;
}


void Optimizer::RemoveAliasFromColumnName(AndList* parseTreeNode)
{
    //this is a single node of AndList
    //just remove the part before "." in each of the orLists
    while(parseTreeNode != NULL)
    {
        OrList* theOrList = parseTreeNode->left;
        while(theOrList != NULL) //to ensure if parse tree contains an OrList and a comparisonOp
        {
            ComparisonOp* theComparisonOp = theOrList->left;
            if(theComparisonOp == NULL)
                break;

            int leftCode = theComparisonOp->left->code;
            string leftVal = theComparisonOp->left->value;
            if(leftCode == NAME)
                strcpy(theComparisonOp->left->value, (char*)leftVal.substr(leftVal.find(".") + 1).c_str());

            //and now the right value
            int rightCode = theComparisonOp->right->code;
            string rightVal = theComparisonOp->right->value;
            if(rightCode == NAME)
                strcpy(theComparisonOp->right->value, (char*)rightVal.substr(rightVal.find(".") + 1).c_str());

            //move to next orList inside first and only AND;
            theOrList = theOrList->rightOr;
        }
        //move to next AndList
        parseTreeNode = parseTreeNode->rightAnd;
    }
}


void Optimizer::FindOptimalPairing(vector<string> & vAliases, AndList* parseTree, 
								   pair<string, string> & pair_optimal)
{
	// Key of this map = <a, b.c> or <b, a.c> or <c, a,b>
	// value = estimation of join tuples
    map<string, double> allEstimates;
    //find all possible combis, sort out best one by looking at numTuples from Stats object
    for(int i = 0; i < vAliases.size(); i++)
    {
        pair<string, string> joinOrder;
        joinOrder.first = vAliases.at(i);
        
        string concat = "";
        for(int j = 0; j < vAliases.size(); j++)
        {
            if(j == i)
                continue;
            concat += vAliases.at(j);
            concat += ".";
        }
        //remove last "." from concat
        concat = concat.substr(0,concat.size()-1);
        joinOrder.second = concat;

        //check if this pair is minimal
        //get Stats object of second, make a copy, apply estimate
        //put all estimates in a map, find minimum and return corresponding
        Statistics *tempStats = new Statistics(*(m_mJoinEstimate[joinOrder.second].stats));
		
		string sOrderedNames = joinOrder.first + "." + joinOrder.second;
        allEstimates[sOrderedNames] = tempStats->Estimate(parseTree, m_aTableNames, vAliases.size()*2);
		//cout << "\n*** order: " << sOrderedNames.c_str();
		//cout << "\t Estimate: "<< allEstimates[sOrderedNames];
        //m_aTableNames is expected to be filled by caller
    }

    map <string, double>::iterator it = allEstimates.begin();
    double minTuples = -1.0;
	string sOptimalOrder;
	for (; it != allEstimates.end(); it++)
    {
        if (minTuples == -1.0)
        {
            minTuples = it->second;
			sOptimalOrder = it->first;
        }
        else
        {
            if(minTuples > it->second)
            {
                minTuples = it->second;
				sOptimalOrder = it->first;
            }
        }
    }

	// break the string sOptimalOrder into optimal_pair
	int dotPos = sOptimalOrder.find(".");
	cout << "\n\n" << sOptimalOrder.c_str();
	if (dotPos == string::npos)
		cerr << "\n\nError in FindOptimalPairs!!\n";
	else
	{
		pair_optimal.first = sOptimalOrder.substr(0, dotPos);
		pair_optimal.second = sOptimalOrder.substr(dotPos+1);
		//cout << "\n-- Optimal Order found (inside): " << pair_optimal.first << ", " 
		//	 << pair_optimal.second << endl;
	}
}

void Optimizer::ConcatSchemas(Schema *pRSch, Schema *pLSch, string sName)
{
    string sFileName = sName + ".schema";
    FILE *outSchemaFile = fopen (sFileName.c_str(), "w");
    fprintf(outSchemaFile, "BEGIN\n%s\nwherever", sName.c_str());
    // go over pRSch and write it to outSchemaFile
    Attribute * pRAtts = pRSch->GetAtts();
    for (int i = 0; i < pRSch->GetNumAtts(); i++)
    {
        fprintf(outSchemaFile, "\n%s", pRAtts[i].name);
        if (pRAtts[i].myType == Int)
            fprintf(outSchemaFile, " Int");
        else if (pRAtts[i].myType == Double)
            fprintf(outSchemaFile, " Double");
        else if (pRAtts[i].myType == String)
             fprintf(outSchemaFile, " String");
    }
    // go over pLSch and write it to outSchemaFile
    Attribute * pLAtts = pLSch->GetAtts();
    for (int i = 0; i < pLSch->GetNumAtts(); i++)
    {
        fprintf(outSchemaFile, "\n%s", pLAtts[i].name);
        if (pLAtts[i].myType == Int)
            fprintf(outSchemaFile, " Int");
        else if (pLAtts[i].myType == Double)
            fprintf(outSchemaFile, " Double");
        else if (pLAtts[i].myType == String)
             fprintf(outSchemaFile, " String");
    }
    // Done
    fprintf (outSchemaFile, "\nEND\n");
    fclose(outSchemaFile);
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
                        case 5:
                                cout<<" < "; break;
                        case 6:
                                cout<<" > "; break;
                        case 7:
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
//

void Optimizer::PrintAndListToUsageMap()
{
    cout << "\n--------m_mAndListToBoolean--------\n";
    map<AndList*, bool>::iterator listToUsageIt = m_mAndListToUsage.begin();
    while(listToUsageIt != m_mAndListToUsage.end())
    {
        PrintAndList(listToUsageIt->first);
        cout << " : " << listToUsageIt->second << "\n";
        listToUsageIt++;
    }
}

void Optimizer::PrintAliasToAndListMap()
{
    cout << "\n--------m_mAliasToAndList--------\n";
    map<string, AndList*>::iterator aliasToAndListIt = m_mAliasToAndList.begin();
    while(aliasToAndListIt != m_mAliasToAndList.end())
    {
        cout << aliasToAndListIt->first << " : " ;
        PrintAndList(aliasToAndListIt->second);
        cout << "\n";
        aliasToAndListIt++;
    }
}
