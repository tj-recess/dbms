/*
 * ErrorLogger.h
 *
 *  Created on: Jan 19, 2011
 *      Author: arpit
 */

#ifndef ERRORLOGGER_H_
#define ERRORLOGGER_H_

#include <fstream>
#include<iostream>

using namespace std;

class EventLogger 
{
private:
	ofstream fout;
	static EventLogger *el;
	EventLogger();
public:
	virtual ~EventLogger();
	void writeLog(const string& msg);
	static EventLogger* getEventLogger();
};

#endif /* ERRORLOGGER_H_ */
