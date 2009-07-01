#ifndef DLLIST_H
#define DLLIST_H

#include <algorithm>

template <class T, size_t Cnt = 1> class dllist {
  T *_prev[Cnt], *_next[Cnt];
public:
  dllist() {
    std::fill(_prev, _prev + Cnt, static_cast<T*>(NULL));
    std::fill(_next, _next + Cnt, static_cast<T*>(NULL));
  }
  bool is_attached(size_t idx = 0) const { return _prev[idx] != NULL; }
  void attach_front(T *&head, size_t idx = 0) {
    assert(_prev[idx] == NULL);
    assert(_next[idx] == NULL);
    if (head == NULL) {
      head = _prev[idx] = _next[idx] = static_cast<T*>(this);
    } else {
      _next[idx] = head;
      _prev[idx] = head->_prev[idx];
      head = _next[idx]->_prev[idx] = _prev[idx]->_next[idx]
	= static_cast<T*>(this);
    }
  }
  void attach_back(T *&head, size_t idx = 0) {
    assert(_prev[idx] == NULL);
    assert(_next[idx] == NULL);
    if (head == NULL) {
      head = _prev[idx] = _next[idx] = static_cast<T*>(this);
    } else {
      _next[idx] = head;
      _prev[idx] = _next[idx]->_prev[idx];
      _prev[idx]->_next[idx] = _next[idx]->_prev[idx] = static_cast<T*>(this);
    }
  }
  T* detach(T *&head, size_t idx = 0) {
    assert(_prev[idx] != NULL);
    assert(_next[idx] != NULL);
    T* n;
    if (head == this) {
      if (_prev[idx] == this) {
	n = head = NULL;
      } else {
	n = head = _next[idx];
      }
    } else {
      n = _next[idx];
    }
    _next[idx]->_prev[idx] = _prev[idx];
    _prev[idx]->_next[idx] = _next[idx];
    _next[idx] = _prev[idx] = NULL;
    return n;
  }
  T* prev(size_t idx = 0) {
    return _prev[idx];
  }
  T* next(size_t idx = 0) {
    return _next[idx];
  }
};

#endif
