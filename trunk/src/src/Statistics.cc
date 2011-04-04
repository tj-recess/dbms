#include "Statistics.h"
#include <iostream>
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
	map <string, int>::iterator atts_itr;
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
    }
    else // if relName found in the RelStats map
    {
		// check if info for "attName" exists
		map <string, int>::iterator att_itr;
		att_itr = (rs_itr->second).Atts.find(string(attName));
		// if no info for this attribute, add it
		if (att_itr == (rs_itr->second).Atts.end())
			(rs_itr->second).Atts[string(attName)] = numDistincts;
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
		TableInfo *pOldTableInfo = &(rs_itr->second);
		
		// make a copy of this OldTableInfo structure
		TableInfo t_info;
		t_info.numTuples = pOldTableInfo->numTuples;
		//t_info.numPartition = pOldTableInfo->numPartition; <-- not sure of this!

		map <string, int>::iterator atts_itr;
		for (atts_itr = pOldTableInfo->Atts.begin();
			 atts_itr != pOldTableInfo->Atts.end();
			 atts_itr++)
		{
			//			attribute name   =>  distinct value
			t_info.Atts[atts_itr->first] = atts_itr->second;
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
}
void Statistics::Write(char *fromWhere)
{
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}

