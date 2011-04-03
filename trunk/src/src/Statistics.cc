#include "Statistics.h"

Statistics::Statistics() : m_nPartitionNum(0)
{}

// Performs a deep copy
Statistics::Statistics(Statistics &copyMe)
{
	m_nPartitionNum = copyMe.GetPartitionNumber();

	// fetch the m_mRelStats of copyMe
	map <string, AttsStats> * pRelStats = copyMe.GetRelStats();

	// now go over pRelStats and copy into m_mRelStats
	map <string, int>::iterator atts_itr;
	map <string, AttsStats>::iterator rs_itr = pRelStats->begin();
	for ( ;rs_itr != pRelStats->end(); rs_itr++)
	{
		// first copy the AttsStats structure
		AttsStats attributes;
		attributes.numTuples = rs_itr->second.numTuples;
		attributes.numPartition = rs_itr->second.numPartition;
		for (atts_itr = rs_itr->second.Atts.begin();
			 atts_itr != rs_itr->second.Atts.end();
			 atts_itr++)
		{
			attributes.Atts[atts_itr->first] = atts_itr->second;
		}

		// now set AttsStats structure for this relation
		m_mRelStats[rs_itr->first] = attributes;
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
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
}
void Statistics::CopyRel(char *oldName, char *newName)
{
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

