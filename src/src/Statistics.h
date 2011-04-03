#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <vector>
#include <string>

using namespace std;

// Structure to store stats of a relation
struct AttsStats
{
	int numTuples;		// total num of rows in this relation
	int numPartition;	// partition this relation belongs to
						// -1 signifies that its still singleton
	map <string, int> Atts;	// <attribute-name, distinct values>

	AttsStats(): numTuples(0), numPartition(-1)
	{}		
};


class Statistics
{
private:
	int m_nPartitionNum;
	map <string, AttsStats> m_mRelStats;
	map <int, vector<string> > m_mPartitionInfoMap;
 	
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	// getter functions
	int GetPartitionNumber()
	{
		return m_nPartitionNum;
	}
	
	map<string, AttsStats> * GetRelStats()
	{
		return &m_mRelStats;	// return pointer to m_mRelStats
	}
	
	map<int, vector<string> > * GetPartitionInfoMap()
	{
		return &m_mPartitionInfoMap; // return pointer to PartitionInfoMap
	}
};

#endif
