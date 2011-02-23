/*
 * ErrorLogger.h
 *
 *  Created on: Jan 19, 2011
 *      Author: arpit
 */

#ifndef EVENTLOGGER_H_
#define EVENTLOGGER_H_

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
	//EventLogger(const EventLogger&);
	virtual ~EventLogger();
	void writeLog(const string& msg);
	static EventLogger* getEventLogger();
};

#endif /* EVENTLOGGER_H_ */