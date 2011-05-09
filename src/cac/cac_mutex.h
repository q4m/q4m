/* Copyright 2009 Cybozu Labs, Inc. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY CYBOZU LABS, INC. ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL CYBOZU LABS, INC. OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of Cybozu Labs, Inc.
 */

#ifndef cac_mutex_h
#define cac_mutex_h

template <typename T> class cac_mutex_t {
public:
  
  class lockref {
  protected:
    cac_mutex_t<T>* m_;
  public:
    lockref(cac_mutex_t<T>& m) : m_(&m) {
      pthread_mutex_lock(&m_->mutex_);
    }
    ~lockref() {
      pthread_mutex_unlock(&m_->mutex_);
    }
    operator T*() { return &m_->t_; }
    T& operator*() { return *operator T*(); }
    T* operator->() { return operator T*(); }
  private:
    lockref(const lockref&);
    lockref& operator=(const lockref&);
  };

protected:
  friend class cac_mutex_t<T>::lockref;
  T t_;
  pthread_mutex_t mutex_;
public:
  cac_mutex_t(const pthread_mutexattr_t* attr) : t_() {
    pthread_mutex_init(&mutex_, attr);
  }
  ~cac_mutex_t() {
    pthread_mutex_destroy(&mutex_);
  }
  const T* unsafe_ref() const { return &t_; }
  T* unsafe_ref() { return &t_; }
  pthread_mutex_t* mutex() { return &mutex_; }
private:
  cac_mutex_t(const cac_mutex_t&);
  cac_mutex_t& operator=(const cac_mutex_t&);
};

#endif
