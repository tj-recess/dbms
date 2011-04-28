#ifndef DDL_DML_H
#define DDL_DML_H

#include "Schema.h"
#include <string>
#include <iostream>
#include <vector>

class DDL_DML
{
private:
	string m_sTabName;
	bool m_bSorted;
	int m_nNumAtts;
	int m_nSortAtts;
	Schema * m_pSchema;			// schema of this table
	OrderMaker * m_pOrderMaker;	// order maker for sorted file
	
public:
	DDL_DML();
	~DDL_DML() {}
	void CreateTable(string sTabName, vector<Attribute> & col_atts_vec, 
					 bool sorted_table = false, vector<string> * sort_col_vec = NULL);
};

#endif
