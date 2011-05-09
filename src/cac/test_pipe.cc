extern "C" {
#include <pthread.h>
#include <unistd.h>
}
#include <cassert>
#include <utility>
#include "cac_mutex.h"
#include "cac_pipe.h"

using namespace std;

struct sum_test {
  cac_pipe_t<int> pipe_;
  int sum_;
  sum_test() : pipe_(NULL, NULL), sum_(0) {}
  void receiver() {
    int v;
    while ((v = pipe_.receive(true)) != 0) {
      sum_ += v;
    }
  }
  static void* receiver(void* self) {
    reinterpret_cast<sum_test*>(self)->receiver();
    return NULL;
  }
  void run() {
    pthread_t tid;
    int err;
    err = pthread_create(&tid, NULL, receiver, this);
    assert(err == 0);
    sleep(1);
    for (int i = 1; i <= 10000; ++i) {
      pipe_.send(i);
    }
    pipe_.send(0);
    err = pthread_join(tid, NULL);
    assert(err == 0);
    assert(sum_ == 50005000);
  }
};

struct timestwo_test {
  cac_pipe_t<pair<int, cac_blocker_t*>*> pipe_;
  timestwo_test() : pipe_(NULL, NULL) {}
  void receiver() {
    pair<int, cac_blocker_t*>* call;
    while ((call = pipe_.receive(true)) != NULL) {
      call->first *= 2;
      call->second->notify();
    }
  }
  static void* receiver(void* self) {
    reinterpret_cast<timestwo_test*>(self)->receiver();
    return NULL;
  }
  void run() {
    pthread_t tid;
    int err;
    err = pthread_create(&tid, NULL, receiver, this);
    assert(err == 0);
    sleep(1);
    cac_blocker_t blocker(NULL, NULL);
    for (int i = 0; i < 10000; ++i) {
      blocker.reset();
      pair<int, cac_blocker_t*> call = make_pair(i, &blocker);
      pipe_.send(&call);
      blocker.wait();
      assert(call.first == i * 2);
    }
    pipe_.send(NULL);
    err = pthread_join(tid, NULL);
    assert(err == 0);
  }
};

int main(int argc, char** argv)
{
  sum_test().run();
  timestwo_test().run();
  return 0;
}
