/*
 * ErrorLogger.cc
 *
 *  Created on: Jan 19, 2011
 *      Author: arpit
 */

#include "ErrorLogger.h"
using namespace std;

ofstream ErrorLogger::fout("errorlog.txt");
ErrorLogger* ErrorLogger::el = NULL;

ErrorLogger::ErrorLogger() 
{}

ErrorLogger::~ErrorLogger()
{}

void ErrorLogger::writeLog(const string& msg)
{
	fout<<msg<<endl;
}

ErrorLogger* ErrorLogger::getErrorLogger()
{
	if(!el)
	{
		el = new ErrorLogger();
	}
	return el;
}
