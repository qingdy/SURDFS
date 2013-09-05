/*
 * Copyright (c) 2011,  Sohu R&D
 * All rights reserved.
 * 
 * File   name: signalset.h
 * Description: implement signal.
 *     
 * Version : 1.0
 * Author  : shiqin@sohu-inc.com
 * Date    : 12-2-6
 *         
 */

#ifndef PANDORA_SYSTEM_SIGNAL_SIGNALSET_H
#define PANDORA_SYSTEM_SIGNAL_SIGNALSET_H
#include <signal.h>

namespace pandora
{

class SignalSet
{
public:
    SignalSet();
    bool Clear();
    bool Fill();
    bool Add(int signo);
    bool Delete(int signo);
    bool IsMember(int signo) const;
    bool IsEmpty() const;
    const sigset_t* Address() const;
    sigset_t* Address();
private:
    sigset_t set_;
};

class Signal
{
public:
    static bool Send(pid_t pid, int signo);
    static bool Send(pid_t pid, int signo, const union sigval value);
    static bool SetMask(const SignalSet& set);
    static bool GetMask(SignalSet* set);
    static bool Block(const SignalSet& set);
    static bool Unblock(const SignalSet& set);
    static void Suspend(const SignalSet& set);
    static bool GetPending(SignalSet* set);
};

}
#endif // PANDORA_SYSTEM_SIGNAL_SIGNALSET_H
