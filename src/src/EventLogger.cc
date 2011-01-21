/*
 * ErrorLogger.cc
 *
 *  Created on: Jan 19, 2011
 *      Author: arpit
 */

#include "ErrorLogger.h"
using namespace std;

EventLogger* EventLogger::el = NULL;

EventLogger::EventLogger() : fout("errorlog.txt"){}

EventLogger::~EventLogger()
{}

void EventLogger::writeLog(const string& msg)
{
	fout<<msg<<endl;
}

EventLogger* EventLogger::getEventLogger()
{
	if(!el)
	{
		el = new EventLogger();
	}
	return el;
}
