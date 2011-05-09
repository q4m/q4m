#ifndef CAC_PIPE_H
#define CAC_PIPE_H

#include <deque>

template <typename Packet> class cac_pipe_t {
protected:
  cac_mutex_t<std::deque<Packet> > pkts_;
  pthread_cond_t cond_;
public:
  cac_pipe_t(const pthread_mutexattr_t* mutexattr, const pthread_condattr_t* condattr) : pkts_(mutexattr) {
    pthread_cond_init(&cond_, condattr);
  }
  ~cac_pipe_t() {
    typename cac_mutex_t<std::deque<Packet> >::lockref pkts(pkts_);
    pkts->clear();
    pthread_cond_destroy(&cond_);
  }
  void clear() {
    typename cac_mutex_t<std::deque<Packet> >::lockref pkts(pkts_);
    pkts->clear();
  }
  void send(const Packet& pkt) {
    typename cac_mutex_t<std::deque<Packet> >::lockref pkts(pkts_);
    pkts->push_back(pkt);
    if (pkts->size() == 1) {
      pthread_cond_signal(&cond_);
    }
  }
  Packet receive(bool blocking) {
    typename cac_mutex_t<std::deque<Packet> >::lockref pkts(pkts_);
    if (blocking) {
      while (pkts->empty()) {
        pthread_cond_wait(&cond_, pkts_.mutex());
      }
    } else {
      if (pkts->empty()) {
        return NULL;
      }
    }
    Packet pkt = *pkts->begin();
    pkts->pop_front();
    return pkt;
  }
private:
  cac_pipe_t(const cac_pipe_t&);
  cac_pipe_t& operator=(const cac_pipe_t&);
};

/**
 * a mix-in class for serialising inter-thread calls (through the use of the
 * wait / notify functions)
 */
class cac_blocker_t {
protected:
  cac_mutex_t<bool> ready_;
  pthread_cond_t cond_;
public:
  cac_blocker_t(const pthread_mutexattr_t* mutexattr, const pthread_condattr_t* condattr) : ready_(mutexattr) {
    pthread_cond_init(&cond_, condattr);
  }
  ~cac_blocker_t() {
    pthread_cond_destroy(&cond_);
  }
  void reset() {
    *cac_mutex_t<bool>::lockref(ready_) = false;
  }
  void wait() {
    cac_mutex_t<bool>::lockref ready(ready_);
    while (! *ready) {
      pthread_cond_wait(&cond_, ready_.mutex());
    }
  }
  void notify() {
    cac_mutex_t<bool>::lockref ready(ready_);
    *ready = true;
    pthread_cond_signal(&cond_);
  }
private:
  cac_blocker_t(const cac_blocker_t&);
  cac_blocker_t& operator=(const cac_blocker_t&);
};

#endif
