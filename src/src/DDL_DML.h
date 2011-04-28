#ifndef DDL_DML_H
#define DDL_DML_H

#include "Schema.h"
#include <string>
#include <iostream>

class DDL_DML
{
private:
	string m_sTabName;
	string m_sTabType;
	int m_nNumAtts;
	char ** m_aSortCols;
	int m_nSortAtts;
	OrderMaker * m_pOrderMaker;
	
public:
	DDL_DML();
	~DDL_DML() {}
	void CreateTable(string sTabName, Attribute* col_atts, 
					int num_atts, string sTabType, 
					char ** sort_cols = NULL, int num_sort_atts = 0);
};

#endif
