/*
 * ErrorLogger.cc
 *
 *  Created on: Jan 19, 2011
 *      Author: arpit
 */

#include "ErrorLogger.h"
using namespace std;

ErrorLogger::ErrorLogger() : fout("errorlog.txt"){}

ErrorLogger::~ErrorLogger()
{

}

void ErrorLogger::writeLog(const string& msg)
{
	fout<<msg<<endl;
}

ErrorLogger* ErrorLogger::getErrorLogger()
{
	if(!ErrorLogger::el)
	{
		ErrorLogger::el = new ErrorLogger();
	}
	return ErrorLogger::el;
}
