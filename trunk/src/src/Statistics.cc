#include "Statistics.h"
#include "EventLogger.h"
#include <string.h>

using namespace std;

Statistics::Statistics() : m_nPartitionNum(0)
{}

// Performs a deep copy
Statistics::Statistics(Statistics &copyMe)
{
	m_nPartitionNum = copyMe.GetPartitionNumber();

	// fetch the m_mRelStats of copyMe
	map <string, TableInfo> * pRelStats = copyMe.GetRelStats();

	// now go over pRelStats and copy into m_mRelStats
	map <string, unsigned long int >::iterator atts_itr;
	map <string, TableInfo>::iterator rs_itr = pRelStats->begin();
	for ( ;rs_itr != pRelStats->end(); rs_itr++)
	{
		// first copy the TableInfo structure
		TableInfo t_info;
		t_info.numTuples = rs_itr->second.numTuples;
		t_info.numPartition = rs_itr->second.numPartition;
		for (atts_itr = rs_itr->second.Atts.begin();
			 atts_itr != rs_itr->second.Atts.end();
			 atts_itr++)
		{
			//          attribute name   =>  distinct value
			t_info.Atts[atts_itr->first] = atts_itr->second;
		}

		// now set TableInfo structure for this relation
		m_mRelStats[rs_itr->first] = t_info;
	}

	// fetch m_mPartitionInfoMap of copyMe
	map <int, vector<string> > * pPartitionInfo = copyMe.GetPartitionInfoMap();

	// now go over pPartitionInfo and copy into m_nPartitionInfoMap
	map <int, vector<string> >::iterator partInfo_itr;
	for (partInfo_itr = pPartitionInfo->begin();
		 partInfo_itr != pPartitionInfo->end(); partInfo_itr++)
	{
		vector<string> vecRelNames;
		vector<string> * pVec = &partInfo_itr->second;
		for (int i = 0; i < pVec->size(); i++)
		{
			vecRelNames.push_back(pVec->at(i));
		}
		
		// now set <part-num, <rel-names...> >
		m_mPartitionInfoMap[partInfo_itr->first] = vecRelNames;
	}

}

Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
	// find relation "relName" in the map
	map <string, TableInfo>::iterator rs_itr;
	rs_itr = m_mRelStats.find(string(relName));

	// if not found in the map, add it
	if (rs_itr == m_mRelStats.end())
	{
		TableInfo t_info;
		t_info.numTuples = numTuples;
		m_mRelStats[string(relName)] = t_info;
	}
	else // just update
	{
		rs_itr->second.numTuples = numTuples;
	}
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	// find relation "relName" in the map
    map <string, TableInfo>::iterator rs_itr;
    rs_itr = m_mRelStats.find(string(relName));

    // if relation is not found in the map, add it
    if (rs_itr == m_mRelStats.end())
    {
		// fill into TableInfo structure
        TableInfo t_info;
		t_info.Atts[string(attName)] = numDistincts;
        //TODO: t_info.numTuples = numDistincts; <-- new info is created

		// add it to the RelStats map
        m_mRelStats[string(relName)] = t_info;

		// add col-table info in m_mColToTable
		vector<string> v_tableName;
		v_tableName.push_back(string(relName));
		m_mColToTable[string(attName)] = v_tableName;
    }
    else // if relName found in the RelStats map
    {
		// check if info for "attName" exists
		map <string, unsigned long int>::iterator att_itr;
		att_itr = (rs_itr->second).Atts.find(string(attName));
		// if no info for this attribute, add it
		if (att_itr == (rs_itr->second).Atts.end())
		{
			(rs_itr->second).Atts[string(attName)] = numDistincts;
			// add col-table info in m_mColToTable
			vector<string> v_tableName;
	        v_tableName.push_back(string(relName));
	        m_mColToTable[string(attName)] = v_tableName;
		}
		else	// update the distinct count
			att_itr->second = numDistincts;
    }
}

void Statistics::CopyRel(char *oldName, char *newName)
{
	// make sure newName doesn't already exist in the map
	map <string, TableInfo>::iterator rs_itr;
	
	#ifdef _STATISTICS_DEBUG
	// Print map	
	for (rs_itr = m_mRelStats.begin();
		 rs_itr != m_mRelStats.end(); rs_itr++)
	{
		cout << "\n Table name: "<< (rs_itr->first).c_str();
	}
	#endif

	// Do 2 things:
	// 1. 	Add new table name to the m_mColToTable
	// 		col_name : <old_table, new_table>
	// 2.	make a copy of this OldTableInfo structure
	//		and associate it with the new table name

	// make iterator for m_mColToTable map
	map <string, vector <string> >::iterator c2t_itr;
	rs_itr = m_mRelStats.find(string(oldName));

	// Fetch old table_info
	TableInfo *pOldTableInfo = &(rs_itr->second);

	// make a copy of this OldTableInfo structure
	TableInfo t_info;
	t_info.numTuples = pOldTableInfo->numTuples;

	// copy attributes and distinct values from map Atts
	map <string, unsigned long int>::iterator atts_itr;
	for (atts_itr = pOldTableInfo->Atts.begin();
					atts_itr != pOldTableInfo->Atts.end();
					atts_itr++)
	{
		//			attribute name   =>  distinct value
		t_info.Atts[atts_itr->first] = atts_itr->second;

		// push new table name for each column
		c2t_itr = m_mColToTable.find(atts_itr->first);
		if (c2t_itr == m_mColToTable.end())
		{
				cerr << "\nColumn name " << (atts_itr->first).c_str()
						<< " not found in m_mColToTable! ERROR!\n";
				return;
		}
		(c2t_itr->second).push_back(string(newName));
	}
	// now set TableInfo structure for this relation
	m_mRelStats[string(newName)] = t_info;

	// NOTE: copyRel will not affect m_mPartitionInfoMap
	// copied relation does not have to belong to old table's partition
}

void Statistics::Read(char *fromWhere)
{
	FILE * fptr = fopen(fromWhere, "r");
	if (fptr == NULL)
		return;

	// this is enough space to hold any tokens
    char buffer[200];
	// other variables
	string relName, attName;
	unsigned long int numValues;
    // make iterator for m_mColToTable map
    map <string, vector <string> >::iterator c2t_itr;

	while (fscanf(fptr, "%s", buffer) != EOF)
	{
		if (strcmp(buffer, "BEGIN") == 0)
		{
			TableInfo t_info;
			fscanf(fptr, "%s", buffer);
			relName = buffer;
			fscanf(fptr, "%lu", &t_info.numTuples);
			fscanf(fptr, "%s", buffer);
			while (strcmp(buffer, "END") != 0)
			{
				attName = buffer;
				fscanf(fptr, "%lu", &numValues);
				t_info.Atts[attName] = numValues;

                // push col-name : table-name into m_mColToTable map
                c2t_itr = m_mColToTable.find(string(attName));
                if (c2t_itr == m_mColToTable.end()) // not found
                {
	                vector<string> v_tableName;
                    v_tableName.push_back(string(relName));
                    m_mColToTable[string(attName)] = v_tableName;
                }
                else // found, add table to vector
    	            (c2t_itr->second).push_back(string(relName));
                                
				// read next row
				fscanf(fptr, "%s", buffer);
			}
			m_mRelStats[relName] = t_info;
		}
	}	
}

void Statistics::Write(char *toWhere)
{
/* 	The file format is:

BEGIN
relName
numTuples
attName distinc-values
attName distinc-values
...
END

BEGIN
relName
numTuples
attName distinc-values
attName distinc-values
...
END
*/

	FILE * fptr = fopen(toWhere, "w");
	map <string, TableInfo>::iterator rs_itr;
	map <string, unsigned long int>::iterator atts_itr;

	// scan through the map
	for (rs_itr = m_mRelStats.begin();
		 rs_itr != m_mRelStats.end(); rs_itr++)
	{
		fprintf(fptr, "\nBEGIN");
		// write relation name
		fprintf(fptr, "\n%s", rs_itr->first.c_str());
		TableInfo * pTInfo = &(rs_itr->second);
		// write number of tuples
		fprintf(fptr, "\n%lu", pTInfo->numTuples);
		// go thru atts map and write attribute-name & distinct value
		for (atts_itr = pTInfo->Atts.begin();
			 atts_itr != pTInfo->Atts.end(); atts_itr++)
		{
			fprintf(fptr, "\n%s", atts_itr->first.c_str());
			fprintf(fptr, " %lu", atts_itr->second);
		}
		fprintf(fptr, "\nEND\n");
	}
	
	// close the file
	fclose(fptr);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	/* Logic:
	 * Call estimate
	 *    Will return new tuple count
     *    Will do error checking
	 * Call checkParseTreeForErrors(), pass it a vector
	 * 	  Will populate this vector with parsed parseTree
	 * Use the vector to find tables being joined (put them in a set)
	 * Set the numTuples to new estimated value of these tables
	 * Change their numPartition
	 * Add new entry in m_mPartitionInfoMap <new-partition-num, <table names> >
	 */

	double new_row_count = Estimate(parseTree, relNames, numToJoin);
	if (new_row_count == -1)	// Error found during estimation
	{
		cerr << "\nError during Estimate(). Cannot proceed with Apply().\n";
		return;
	}
	else
	{
		// create a set of table names, to which these attributes belong
        set <string> join_table_set;
		vector<string> v_joinAtts;
		if (!checkParseTreeForErrors(parseTree, relNames, numToJoin, v_joinAtts, join_table_set))
		{
			cerr << "\nError during CNF parsing. Cannot proceed with Apply().\n";
			return;
		}
		else
		{
/*			// create a set of table names, to which these attributes belong
			set <string> join_table_set;
			string sTableName;

                        for(int i = 0; i < v_joinAtts.size(); i+=3)
                        {
                            if(v_joinAtts.at(i).compare("4") == 0)
                            {
				string sColName = v_joinAtts.at(i+1);
				int prefixedTabNamePos = sColName.find(".");
				if (prefixedTabNamePos != string::npos)		// table name is prefixed with col-name
					sTableName = sColName.substr(0, prefixedTabNamePos);
				else
					sTableName = m_mColToTable[sColName].at(0);	// first table this col is associated with
				
				// push this table in the set
				join_table_set.insert(sTableName);
                            }

			}*/

			// At this point join_table_set contains the tables to be joined
			// So change their partition info
			// New partition number
			m_nPartitionNum++;
			vector <string> vTableNames;
			map <string, TableInfo>::iterator rs_itr;
			set <string>::iterator set_itr = join_table_set.begin();
			for (; set_itr != join_table_set.end(); set_itr++)
			{
				rs_itr = m_mRelStats.find(*set_itr);
				if (rs_itr == m_mRelStats.end())
				{
					cerr << "\n Table " << (*set_itr).c_str() << " details not found! error! \n";
					return;
				}
				(rs_itr->second).numPartition = m_nPartitionNum;
				(rs_itr->second).numTuples = (unsigned long int)new_row_count;
	
				// push this table name in vTableNames vector (used after this loop)
				vTableNames.push_back(*set_itr);
			}
			// add new entry in m_mPartitionInfoMap
			m_mPartitionInfoMap[m_nPartitionNum] = vTableNames;
		}
	}
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    for (int i = 0; i < numToJoin; i++)
        cout<< relNames[i] << endl;
    set <string> dummy;
    vector<string> joinAttsInPair;
    if(checkParseTreeForErrors(parseTree, relNames, numToJoin, joinAttsInPair, dummy))
        cout<< "\nsuccessfully parsed" << endl;
    else
    {
        cout<< "\ninvalid parseTree" << endl;
        return -1;
    }
//    parseTree->left = copyLeft;

#ifdef _STATISTICS_DEBUG
    for(int i = 0; i < joinAttsInPair.size(); i++)
        cout << joinAttsInPair[i] << " ";
    cout<<"\n|||||\n";
#endif

    //start performing join and applying stats
    /**
     * Algorithm:
     * 1. check for the partitions which we are gonna join
     * 2. create/change partitions accordingly
     * 3. estimate tuples for every new partition created
     * 4. repeat step 3 until relNames is exhausted
     * return the final estimate
     */

    set <string> join_table_set;
    vector<long double> estimates;
    string last_connector = "";
    int i = 0;
    long double prvsEstimate = 1;
    while(i < joinAttsInPair.size())
    {
        long double localEstimate = -1;

        int col1Type = atoi(joinAttsInPair.at(i++).c_str());
        string col1Val = joinAttsInPair.at(i++);
        int operatorCode = atoi(joinAttsInPair.at(i++).c_str());
        int col2Type = atoi(joinAttsInPair.at(i++).c_str());
        string col2Val = joinAttsInPair.at(i++);
        string connector = joinAttsInPair.at(i++);

        string tab1;
        int prefixedTabNamePos;
        if(col1Type == NAME)
        {
            prefixedTabNamePos = col1Val.find(".");
            if(prefixedTabNamePos != string::npos)
                tab1 = col1Val.substr(0, prefixedTabNamePos);
            else
                tab1 = m_mColToTable[col1Val].at(0);

            join_table_set.insert(tab1);
        }
        


        string tab2;
        if(col2Type == NAME)
        {
            prefixedTabNamePos = col2Val.find(".");
            if(prefixedTabNamePos != string::npos)
                tab2 = col2Val.substr(0, prefixedTabNamePos);
            else
                tab2 = m_mColToTable[col2Val].at(0);

            join_table_set.insert(tab2);
        }

        if(col1Type == NAME && col2Type == NAME)    //join condition
        {
            TableInfo t1;
            TableInfo t2;
            //find localEstimate
            t1 = m_mRelStats[tab1];
            t2 = m_mRelStats[tab2];

            localEstimate = 1.0/(max(t1.Atts[col1Val], t2.Atts[col2Val]));

            //as this is a join condition so we are expecting it to be in a separate AndList
            //so no need to check the connector (AND, OR or .); therefore push this estimate onto vector
            estimates.push_back(localEstimate);
        }
        else if(col1Type == NAME || col2Type == NAME)
        {
            TableInfo t;
            string colName;
            if(col1Type == NAME)
            {
                t = m_mRelStats[tab1];
                colName = col1Val;
            }
            else
            {
                t = m_mRelStats[tab2];
                colName = col2Val;
            }
            if(operatorCode == EQUALS)
            {
                if(connector.compare("OR") == 0 || last_connector.compare("OR") == 0)
                {
                    localEstimate = (1.0- 1.0/t.Atts[colName]);
                    if(connector.compare("OR") != 0)    //i.e. it's either AND or "." so current orList is done
                    {
                        //compute all estimates and load onto vector
                        //using the formula [1.0 - (1 - 1/f1)(1-1/f2)...]
                        long double totalCurrentEstimate = 1.0 - prvsEstimate*localEstimate;
                        estimates.push_back(totalCurrentEstimate);
                        //re-init prvsEstimate
                        prvsEstimate = 1;
                    }
                    else    //someone is still left in the orList
                    {
                        prvsEstimate = prvsEstimate*localEstimate;
                    }
                }
                else
                {
                    localEstimate = 1.0/t.Atts[colName];
                    estimates.push_back(localEstimate);
                    //re-init prvsEstimate
                    prvsEstimate = 1;
                }
            }
            else    // operator is either > or <
            {
                if(connector.compare("OR") == 0 || last_connector.compare("OR"))
                {
                    localEstimate = (1.0 - 1.0/3);
                    if(connector.compare("OR") != 0)    //i.e. it's either AND or "." so current orList is done
                    {
                        //compute all estimates and load onto vector
                        int totalCurrentEstimate = 1.0/(1.0 - prvsEstimate*localEstimate);
                        estimates.push_back(totalCurrentEstimate);
                    }
                    else    //someone is still left in the orList
                    {
                        prvsEstimate = prvsEstimate*localEstimate;
                    }
                }
                else
                {
                    localEstimate = (1.0/3);
                    estimates.push_back(localEstimate);
                }
            }
        }
        else    //this is literal = literal kind of comparison
        {
            //TODO
        }
        last_connector = connector;
    }
    //done with all computation, now return the estimation
    
    //compute the numerator
    unsigned long long int numerator = 1;
    set <string>::iterator set_itr = join_table_set.begin();
    for (; set_itr != join_table_set.end(); set_itr++)
        numerator *= m_mRelStats[*set_itr].numTuples;

    double result = numerator;
    for(int i = 0; i < estimates.size(); i++)
        result *= estimates.at(i);

    return result;
}

//parse the parseTree and check for errors
bool Statistics::checkParseTreeForErrors(struct AndList *someParseTree, char *relNames[], 
										 int numToJoin, vector<string>& joinAttsInPair,
										 set<string>& table_names_set)
{
    /**
     * 1. check if stats obj even contains the relations in relNames
     * 2. //check for unambiguity of column names (not required as of now with TPCH)
     * 3. try to match columns in parseTree with the relNames: if matched, go ahead. else, print error and exit.
     * 3.5 also check if same column name without prefix matches more than one tables in the m_mColToTable map
     * 4. check if the relNames contains relation in complete partition maintained by stats obj
     * 5. check for column types (to make sure string is joined with string and so on...)   //TODO:not doing right now
     */

    //1. check if stats obj even contains the relations in relNames
    for (int i = 0; i < numToJoin; i++)
    {
        if(m_mRelStats.find(relNames[i]) == m_mRelStats.end())
            return false;
    }

	// some variables needed
	int prefixedTabNamePos;
	string sTableName, sColName;
	map<string, vector<string> >::iterator c2t_itr;

    //first create copy of passed AndList
    AndList* parseTree = someParseTree;
    
    //3. try to match columns in parseTree with the relNames: if matched, go ahead. else, print error and exit.
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

            joinAttsInPair.push_back(System::my_itoa(leftCode));
            joinAttsInPair.push_back(leftVal);   //remember to apply itoa before using double or int

            //now push the operator
            joinAttsInPair.push_back(System::my_itoa(theComparisonOp->code));

            //and now the right value
            int rightCode = theComparisonOp->right->code;
            string rightVal = theComparisonOp->right->value;

            joinAttsInPair.push_back(System::my_itoa(rightCode));
            joinAttsInPair.push_back(rightVal);   //remember to apply itoa before using double or int

            if(leftCode == NAME)    //check if column belongs to some table or not
            {
                //also check if the column name has "table_name." prefixed with it
                prefixedTabNamePos = leftVal.find(".");
                if (prefixedTabNamePos != string::npos)     // table_name.col_name format
                {
                    sTableName = leftVal.substr(0, prefixedTabNamePos);
                    sColName = leftVal.substr(prefixedTabNamePos + 1);
                }
                else
                {
                    // column name is not prefixed by table_name "."
                    // so check if column has more than one associated tables
                    if ((c2t_itr->second).size() > 1)
                            return false;	// ambiguity error!
                    else
                            sTableName = (c2t_itr->second).at(0);

                    sColName = leftVal;
                }
                table_names_set.insert(sTableName);

                c2t_itr = m_mColToTable.find(sColName);
                if (c2t_itr == m_mColToTable.end())	// column not found
                    return false;

            }
            if(rightCode == NAME)   //check if column belongs to some table or not
            {
                //also check if the column name has "table_name." prefixed with it
                prefixedTabNamePos = leftVal.find(".");
                if (prefixedTabNamePos != string::npos)     // table_name.col_name format
                {
                    sTableName = leftVal.substr(0, prefixedTabNamePos);
                    sColName = leftVal.substr(prefixedTabNamePos + 1);
                }
                else
                {
                    // column name is not prefixed by table_name "."
                    // so check if column has more than one associated tables
                    if ((c2t_itr->second).size() > 1)
                        return false;   // ambiguity error!
                    else
                        sTableName = (c2t_itr->second).at(0);

                    sColName = rightVal;
                }
                table_names_set.insert(sTableName);

                c2t_itr = m_mColToTable.find(sColName);
                if (c2t_itr == m_mColToTable.end()) // column not found
                    return false;
            }

            //move to next orList inside first AND
            if(theOrList->rightOr != NULL)
                joinAttsInPair.push_back("OR");
            theOrList = theOrList->rightOr;
        }
        //move to next AndList node of the parseTree
        if(parseTree->rightAnd != NULL)
            joinAttsInPair.push_back("AND");
        else
            joinAttsInPair.push_back(".");
        parseTree = parseTree->rightAnd;
    }
    
	// Now check if the tables being joined (from ParseTree) makes sense
	// with respect to the partitions	
	for (int i=0; i<numToJoin; i++)
	{
		sTableName = relNames[i];
		int partitionNum = m_mRelStats[sTableName].numPartition;
		if (partitionNum != -1)
		{
			// now iterate over all tables in this partition
			// and see if all tables exist in relNames
			vector<string> v_tbl_names = m_mPartitionInfoMap[partitionNum];
			for (int j = 0; j < v_tbl_names.size(); j++)
			{
				string t1 = v_tbl_names.at(i);
				bool found = false;
				for (int k = 0; k < numToJoin; k++)
				{
					string t2 = relNames[k];
					if (t1.compare(t2) == 0)
					{
						found = true;
						break;
					}
				}	
				if (found == false)
					return false;		// table belonging to a partition not found in relNames
			}
		}
	}

	// Also check if table_names_set contains table names that are in relNames
	set <string>::iterator set_itr = table_names_set.begin();
    for (; set_itr != table_names_set.end(); set_itr++)
	{
		string t1 = *set_itr;
		bool found = false;
        for (int k = 0; k < numToJoin; k++)
		{
        	string t2 = relNames[k];
            if (t1.compare(t2) == 0)
            {   
            	found = true;
                break;
            }
		}
        if (found == false)
        	return false;       // table belonging to a partition not found in relNames
	}

    //control is here that means parseTree passed every test
    return true;
}
