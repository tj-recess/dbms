#ifndef STATISTICS_
#define STATISTICS_

#define _STATISTICS_DEBUG 0

#include "ParseTree.h"
#include <map>
#include <vector>
#include <algorithm>
#include <string>
#include <fstream>
#include <iostream>
using namespace std;

// Structure to store stats of a relation
struct TableInfo
{
	unsigned long int numTuples;			// total num of rows in this relation
	int numPartition;		// partition this relation belongs to
							// -1 signifies that its still singleton
	map <string, unsigned long int> Atts;	// <attribute-name, distinct values>

	TableInfo(): numTuples(0), numPartition(-1)
	{}		
};


class Statistics
{
private:
	int m_nPartitionNum;
	map <string, TableInfo> m_mRelStats;
	map <int, vector<string> > m_mPartitionInfoMap;
        //returns true if parseTree is error free, false otherwise
        bool checkParseTreeForErrors(struct AndList *parseTree, char *relNames[], int numToJoin, vector<string>&);
	map <string, vector <string> > m_mColToTable;
 	
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
	
	map<string, TableInfo> * GetRelStats()
	{
		return &m_mRelStats;	// return pointer to m_mRelStats
	}
	
	map<int, vector<string> > * GetPartitionInfoMap()
	{
		return &m_mPartitionInfoMap; // return pointer to PartitionInfoMap
	}
};

#endif
