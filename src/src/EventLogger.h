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
#include <sys/time.h>
#include <sstream>

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

class System
{
public:
    static string getusec()
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        stringstream ss;
        ss << tv.tv_sec;
        ss << ".";
        ss << tv.tv_usec;
        return ss.str();
    }

};

#endif /* EVENTLOGGER_H_ */
