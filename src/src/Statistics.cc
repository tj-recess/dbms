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
	rs_itr = m_mRelStats.find(string(newName));
	if (rs_itr != m_mRelStats.end())
	{
		cerr << "\n" << newName << " already exists! Can't make a copy "
			 << "with this name. Please choose a new name!\n";
		return;
	}
	else
	{
		// Do 2 things:
		// 1. 	Add new table name to the m_mColToTable
		// 		col_name : <old_table, new_table>
		// 2.	make a copy of this OldTableInfo structure
		//		and associate it with the new table name

		// make iterator for m_mColToTable map
		map <string, vector <string> >::iterator c2t_itr;

		// Fetch old table_info
		TableInfo *pOldTableInfo = &(rs_itr->second);
		
		// make a copy of this OldTableInfo structure
		TableInfo t_info;
		t_info.numTuples = pOldTableInfo->numTuples;
		//t_info.numPartition = pOldTableInfo->numPartition; <-- not sure of this!

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

		
	}

	// TODO: how does this operation affect the m_mPartitionInfoMap?
	// new rel belongs to the same partition as the old one?
	// or to a singleton partition?
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

/**
 *
 * @param parseTree
 * @param relNames
 * @param numToJoin
 */
void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{


    /*
    vector<string> newPartitionTabs;
    int newPartitionNum = ++m_nPartitionNum;
    for(int i = 0; i < numToJoin; i++)
    {
        if(m_mRelStats[relNames[i]].numPartition == -1)
        {
            newPartitionTabs.push_back(relNames[i]);
            m_mRelStats[relNames[i]].numPartition = newPartitionNum;
        }
    }
    //create a new partition with the vector
    m_mPartitionInfoMap[newPartitionNum] = newPartitionTabs;

    int estimatedTuples = 0;
    for(int i = 0; i < newPartitionTabs.size(); i+=2)
    {
        TableInfo t1 = m_mRelStats[newPartitionTabs.at(i)];
        TableInfo t2 = m_mRelStats[newPartitionTabs.at(i+1)];
        estimatedTuples = t1.numTuples * t2.numTuples / max(t1.Atts[parseTree->left->left->left->value], t1.Atts[parseTree->left->left->right->value]);
    }
    */

}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    for (int i = 0; i < numToJoin; i++)
        cout<< relNames[i] << endl;

    vector<string> joinAttsInPair;
    if(checkParseTreeForErrors(parseTree, relNames, numToJoin, joinAttsInPair))
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

    
}

bool Statistics::checkParseTreeForErrors(struct AndList *someParseTree, char *relNames[], int numToJoin, vector<string>& joinAttsInPair)
{
    //parse the parseTree and check for errors
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

    //first create copy of passed AndList
    AndList* parseTree = someParseTree;
    
    //3. try to match columns in parseTree with the relNames: if matched, go ahead. else, print error and exit.
    while(parseTree != NULL)
    {
        OrList* theOrList = parseTree->left;
        while(theOrList != NULL) //to ensure if parse tree contains an OrList and a comparisonOp
        {
            ComparisonOp* theComparisonOp = parseTree->left->left;
            if(theComparisonOp == NULL)
                break;
            //first push the left value
            int leftCode = theComparisonOp->left->code;
            char* leftVal = theComparisonOp->left->value;

            joinAttsInPair.push_back(System::my_itoa(leftCode));
            joinAttsInPair.push_back(leftVal);   //remember to apply itoa before using double or int

            //now push the operator
            joinAttsInPair.push_back(System::my_itoa(theComparisonOp->code));

            //and now the right value
            int rightCode = theComparisonOp->right->code;
            char* rightVal = theComparisonOp->right->value;

            joinAttsInPair.push_back(System::my_itoa(rightCode));
            joinAttsInPair.push_back(rightVal);   //remember to apply itoa before using double or int

            if(leftCode == NAME)    //check if column belongs to some table or not
            {
                if(m_mColToTable.find(leftVal) == m_mColToTable.end())
                    return false;
                //also check if the column name has "table_name." prefixed in it
            }
            if(rightCode == NAME)   //check if column belongs to some table or not
            {
                if(m_mColToTable.find(rightVal) == m_mColToTable.end())
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
    

    //control is here that means parseTree passed every test
    return true;
}